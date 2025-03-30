import PromisePool from "@supercharge/promise-pool";
import { FeatherDocument, FeatherDocumentEntry, IngestionDocument } from "./FeatherDocumentStore.d";
import { sha256 } from "js-sha256";
import { compress, decompress, IBrotliCompressOptions } from 'brotli-compress'
import { parse, stringify } from "uuid";

export abstract class FeatherDocumentStore
{

    /**
     * The name of the collection in the database.
     **/
    public indexName: string;

    MAX_CONCURRENT = 4;

    COMPRESSION_OPTIONS : IBrotliCompressOptions = {
        quality: 4, // 0-11, 11 is the best compression but slowest, 4 was discovered by cloudflare to be the best tradeoff between speed and compression
    } 

    /**
     * Whether to enable compression for the text field.
     * If true, the text field will be compressed using Brotli.
     * If false, the text field will be stored as plain text.
     **/
    public enableCompression: boolean = true;

    constructor(indexName: string, enableCompression: boolean = true)
    {
        this.indexName = indexName;
        this.enableCompression = enableCompression;
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
            const compressed_text_buffer = this.enableCompression ? await compress(text_buffer, this.COMPRESSION_OPTIONS) : text_buffer;

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
        await this.insert_internal(results);
    }

    async delete(ids: Uint8Array | Uint8Array[]): Promise<void>
    {
        // Ensure documents is an array
        if (!Array.isArray(ids)) {
            ids = [ids];
        }
        if(ids.length === 0) throw new Error("No documents to delete");

        //Adapter is responsible for deleting the entries in the data store
        await this.delete_internal(ids);
    }


    async document_exists(documents: IngestionDocument[] | IngestionDocument): Promise<(FeatherDocumentEntry | undefined)[]>
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

    async get(uuid: string): Promise<FeatherDocument | undefined>
    {
        const uuidBytes = parse(uuid);
        const compressed_document = await this.get_document_by_uuid(uuidBytes);

        if(compressed_document === undefined) return undefined;

        const decompressed_text = this.enableCompression ? Buffer.from(await decompress(compressed_document.txt)) : Buffer.from(compressed_document.txt);
        const text = decompressed_text.toString('utf-8');

        const sha_string = SHAToHexString(compressed_document.sha);

        return {
            pk: this.indexName,
            id: uuid,
            sha: sha_string,
            title: compressed_document.t,
            text: text,
            published: compressed_document.p,
        } as FeatherDocument;
    }

    //TODO: Implement DynamoDB Table for DocumentStore
    abstract get_document_by_sha(shas: ArrayBuffer[]) : Promise<(FeatherDocumentEntry | undefined)[]>;
    abstract get_document_by_uuid(uuid: Uint8Array) : Promise<FeatherDocumentEntry | undefined>;

    abstract search_by_title(title: string): Promise<FeatherDocumentEntry[]>;
    
    abstract insert_internal(documents: FeatherDocumentEntry[]) : Promise<Uint8Array[]>;
    abstract delete_internal(uuids: Uint8Array[]) : Promise<Uint8Array[]>;
}   

export function SHAToHexString(array : Uint8Array): string {
    return Array.from(array)
      .map(byte => byte.toString(16).padStart(2, '0'))
      .join('');
}