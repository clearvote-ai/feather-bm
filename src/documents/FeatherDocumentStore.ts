import PromisePool from "@supercharge/promise-pool";
import { FeatherDocument, FeatherDocumentEntry, IngestionDocument } from "./FeatherDocumentStore.d";
import { sha256 } from "js-sha256";
import { compress, decompress, IBrotliCompressOptions } from 'brotli-compress'
import { parse, stringify } from "uuid";
import { FeatherBMIndex } from "../search/FeatherBMIndex";

export abstract class FeatherDocumentStore
{

    /**
     * The name of the collection in the database.
     **/
    public indexName: string;

    MAX_CONCURRENT = 4;

    COMPRESSION_OPTIONS : IBrotliCompressOptions = {
        quality: 11,
    } 

    featherIndex: FeatherBMIndex;

    constructor(indexName: string, featherIndex: FeatherBMIndex)
    {
        this.indexName = indexName;
        this.featherIndex = featherIndex;
    }

    async insert(documents: IngestionDocument[] | IngestionDocument): Promise<void>
    {
        // Ensure documents is an array
        if (!Array.isArray(documents)) {
            documents = [documents];
        }
        if(documents.length === 0) throw new Error("No documents to insert");

        const { results } = await PromisePool
        .withConcurrency(this.MAX_CONCURRENT)
        .for(documents)
        .process(async (doc, index) => {
            const text_buffer = Buffer.from(doc.text);
            const compressed_text_buffer = await compress(text_buffer, this.COMPRESSION_OPTIONS);

            const sha = new Uint8Array(sha256.arrayBuffer(doc.text));

            const uuid = parse(doc.uuidv7);

            return {
                id: uuid,
                t: doc.title,
                txt: compressed_text_buffer,
                sha: sha,
                p: doc.published,
            } as FeatherDocumentEntry;
        });

        //Adapter is responsible for inserting the entries in the data store
        const inserted_entries = await this.insert_internal(results as FeatherDocumentEntry[]);

        const successfully_inserted_docs = documents.filter((doc, index) => {
            return inserted_entries[index] !== null;
        });

        //Index the documents
        await this.featherIndex.insert(successfully_inserted_docs);

    }

    async delete(ids: Uint8Array | Uint8Array[]): Promise<void>
    {
        // Ensure documents is an array
        if (!Array.isArray(ids)) {
            ids = [ids];
        }
        if(ids.length === 0) throw new Error("No documents to delete");

        //Adapter is responsible for deleting the entries in the data store
        const removed_entries = await this.delete_internal(ids);

        const removed_documents = removed_entries.map((entry) => {
            const decompressed_text = decompress(entry.txt);
            return {
                uuidv7: stringify(entry.id),
                title: entry.t,
                text: decompressed_text.toString(),
                published: entry.p,
            } as IngestionDocument;
        });

        //de index the documents
        await this.featherIndex.delete(removed_documents);
    }


    async document_exists(documents: IngestionDocument[] | IngestionDocument): Promise<(FeatherDocumentEntry | null)[]>
    {
        // Ensure documents is an array
        if (!Array.isArray(documents)) {
            documents = [documents];
        }
        if(documents.length === 0) throw new Error("No documents to compare");
        const shas = documents.map(doc => sha256.arrayBuffer(doc.text));
        const sha_matches = await this.get_document_by_sha(shas);

        return sha_matches;
    }

    async get(uuid: string): Promise<FeatherDocument | null>
    {
        const uuidBytes = parse(uuid);
        const compressed_document = await this.get_document_by_uuid(uuidBytes);

        if(compressed_document === null) return null;

        const decompressed_text = await decompress(compressed_document.txt);
        const text = decompressed_text.toString(); 

        return {
            id: uuid,
            sha: compressed_document.sha,
            title: compressed_document.t,
            text: text,
            published: compressed_document.p,
        } as FeatherDocument;
    }

    abstract get_document_by_sha(shas: ArrayBuffer[]) : Promise<(FeatherDocumentEntry | null)[]>;
    abstract get_document_by_uuid(uuid: Uint8Array) : Promise<FeatherDocumentEntry | null>;

    abstract search_by_title(title: string): Promise<FeatherDocumentEntry[]>;
    abstract search_by_text(query: string): Promise<FeatherDocumentEntry[]>;

    
    abstract insert_internal(documents: FeatherDocumentEntry[]) : Promise<FeatherDocumentEntry[]>;
    abstract delete_internal(uuids: Uint8Array[]) : Promise<FeatherDocumentEntry[]>;
}   