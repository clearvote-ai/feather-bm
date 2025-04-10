import PromisePool from "@supercharge/promise-pool";
import { FeatherDocument, FeatherDocumentEntry, IngestionDocument } from "./FeatherDocumentStore.d";
import { sha256 } from "js-sha256";
import { compress, decompress, IBrotliCompressOptions } from 'brotli-compress'
import { parse, stringify } from "uuid";

export abstract class FeatherDocumentStore
{


    //maximum number of concurrent requests to the data store
    //adjust based on your API limits
    MAX_CONCURRENT = 4;

    COMPRESSION_OPTIONS : IBrotliCompressOptions = {
        //https://blog.cloudflare.com/results-experimenting-brotli/
        //4 was discovered by cloudflare to be the best tradeoff between speed and compression
        //we set 3 because our average document is small, your mileage may vary
        quality: 3, // 0-11, 11 is the best compression but slowest
    } 

    /**
     * Whether to enable compression for the text field.
     * If true, the text field will be compressed using Brotli.
     * If false, the text field will be stored as plain text.
     **/
    public enableCompression: boolean = true;

    constructor(enableCompression: boolean = true)
    {
        this.enableCompression = enableCompression;
    }

    async insert(documents: IngestionDocument[] | IngestionDocument, indexName: string): Promise<void>
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
        await this.insert_internal(results, indexName);
    }

    async delete(ids: Uint8Array | Uint8Array[], indexName: string): Promise<void>
    {
        // Ensure documents is an array
        if (!Array.isArray(ids)) {
            ids = [ids];
        }
        if(ids.length === 0) throw new Error("No documents to delete");

        //Adapter is responsible for deleting the entries in the data store
        await this.delete_internal(ids, indexName);
    }


    async document_exists(documents: IngestionDocument[] | IngestionDocument, indexName: string): Promise<(FeatherDocumentEntry | undefined)[]>
    {
        // Ensure documents is an array
        if (!Array.isArray(documents)) {
            documents = [documents];
        }
        if(documents.length === 0) throw new Error("No documents to compare");
        const shas = documents.map(doc => sha256.arrayBuffer(doc.text));
        const sha_matches = await this.get_document_by_sha(shas, indexName);

        return sha_matches;
    }

    async get(uuid: string, indexName: string): Promise<FeatherDocument | undefined>
    {
        const uuidBytes = parse(uuid);
        const compressed_document = await this.get_document_by_uuid(uuidBytes, indexName);

        if(compressed_document === undefined) return undefined;

        const decompressed_text = this.enableCompression ? Buffer.from(await decompress(compressed_document.txt)) : Buffer.from(compressed_document.txt);
        const text = decompressed_text.toString('utf-8');

        const sha_string = SHAToHexString(compressed_document.sha);

        return {
            pk: indexName,
            id: uuid,
            sha: sha_string,
            title: compressed_document.t,
            text: text,
            published: compressed_document.p,
        } as FeatherDocument;
    }

    //TODO: Implement DynamoDB Table for DocumentStore
    abstract get_document_by_sha(shas: ArrayBuffer[], indexName: string) : Promise<(FeatherDocumentEntry | undefined)[]>;
    abstract get_document_by_uuid(uuid: Uint8Array, indexName: string) : Promise<FeatherDocumentEntry | undefined>;

    abstract search_by_title(title: string, indexName: string): Promise<FeatherDocumentEntry[]>;
    
    abstract insert_internal(documents: FeatherDocumentEntry[], indexName: string) : Promise<Uint8Array[]>;
    abstract delete_internal(uuids: Uint8Array[], indexName: string) : Promise<Uint8Array[]>;
}   

export function SHAToHexString(array : Uint8Array): string {
    return Array.from(array)
      .map(byte => byte.toString(16).padStart(2, '0'))
      .join('');
}