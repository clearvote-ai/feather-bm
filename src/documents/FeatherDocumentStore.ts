import PromisePool from "@supercharge/promise-pool";
import { FeatherDocument, FeatherDocumentEntry, IngestionDocument } from "./FeatherDocumentStore.d";
import { sha256 } from "js-sha256";
import { compress, decompress, IBrotliCompressOptions } from 'brotli-compress'
import { parse, stringify } from "uuid";

export abstract class FeatherDocumentStore
{
    MAX_CONCURRENT_COMPRESSION_THREADS = 1;

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

    async insert<F extends FeatherDocument>(documents: F[] | F, collectionName: string): Promise<void>
    {
        // Ensure documents is an array
        if (!Array.isArray(documents)) { 
            documents = [documents];
        }
        if(documents.length === 0) throw new Error("No documents to insert");

        const { results } = await PromisePool
        .withConcurrency(this.MAX_CONCURRENT_COMPRESSION_THREADS)
        .for(documents)
        .process(async (doc, index) => {
            const text_buffer = Buffer.from(doc.text);
            const compressed_text_buffer = this.enableCompression ? await compress(text_buffer, this.COMPRESSION_OPTIONS) : text_buffer;

            const sha = new Uint8Array(sha256.arrayBuffer(doc.text));

            const uuid = parse(doc.id);

            return {
                pk: collectionName,
                id: uuid,
                t: doc.title,
                txt: compressed_text_buffer,
                sha: sha,
                p: doc.published
            } satisfies FeatherDocumentEntry;
        });

        //Adapter is responsible for inserting the entries in the data store
        await this.insert_internal(results, collectionName);
    }

    async delete(ids: string | string[], collectionName: string): Promise<void>
    {
        // Ensure documents is an array
        if (!Array.isArray(ids)) {
            ids = [ids];
        }
        if(ids.length === 0) throw new Error("No documents to delete");

        const id_buffer = ids.map(id => parse(id));

        //Adapter is responsible for deleting the entries in the data store
        await this.delete_internal(id_buffer, collectionName);
    }


    async document_exists(documents: FeatherDocument[] | FeatherDocument, collectionName: string): Promise<(FeatherDocumentEntry | undefined)[]>
    {
        // Ensure documents is an array
        if (!Array.isArray(documents)) {
            documents = [documents];
        }
        if(documents.length === 0) throw new Error("No documents to compare");
        const shas = documents.map(doc => sha256.arrayBuffer(doc.text));
        const sha_matches = await this.get_document_by_sha(shas, collectionName);

        return sha_matches;
    }

    async bulk_get(uuids: string[], collectionName: string): Promise<FeatherDocument[]>
    {
        const uuid_buffer = uuids.map(uuid => parse(uuid));
        const entries = await this.bulk_get_document_by_uuid(uuid_buffer, collectionName);
        const decompressed_entries = await Promise.all(entries.map(async (entry) => {
            if(entry === undefined) return undefined;
            return await this.decompressFeatherDocumentEntry(entry, collectionName);
        })
        // Filter out undefined entries
        .filter((entry) => entry !== undefined));
        
        return decompressed_entries as FeatherDocument[];
    }

    async get<F extends FeatherDocument>(uuid: string, collectionName: string): Promise<F | undefined>
    {
        const uuidBytes = parse(uuid);
        const compressed_document = await this.get_document_by_uuid(uuidBytes, collectionName);
        if(compressed_document === undefined) return undefined;

        return await this.decompressFeatherDocumentEntry(compressed_document, collectionName);
    }

    async decompressFeatherDocumentEntry<F extends FeatherDocument>(compressed_document: FeatherDocumentEntry, collectionName: string): Promise<F | undefined>
    {
        if(compressed_document === undefined) return undefined;

        const decompressed_text = this.enableCompression ? Buffer.from(await decompress(compressed_document.txt)) : Buffer.from(compressed_document.txt);
        const text = decompressed_text.toString('utf-8');

        const sha_string = SHAToHexString(compressed_document.sha);

        const uuid = compressed_document.id;
        const uuid_string = stringify(uuid);

        const custom_field_keys = Object.keys(compressed_document).filter((key) => !["pk", "id", "sha", "t", "txt", "p"].includes(key));
        const custom_fields = custom_field_keys.reduce((acc, key) => {
            acc[key] = (compressed_document as Record<string, any>)[key];
            return acc;
        }, {} as Record<string, any>);

        return {
            pk: collectionName,
            id: uuid_string,
            sha: sha_string,
            title: compressed_document.t,
            text: text,
            published: compressed_document.p,
            //copy custom fields from the document
            ...custom_fields,
        } as F;
    }

    async searchByTitle<F extends FeatherDocument>(title: string, collectionName: string): Promise<F[]>
    {
        const entries = await this.search_by_title(title, collectionName);
        const decompressed_entries = await Promise.all(entries.map(async (entry) => {
            if(entry === undefined) return undefined;
            return await this.decompressFeatherDocumentEntry(entry, collectionName);
        })
        // Filter out undefined entries
        .filter((entry) => entry !== undefined));
        
        return decompressed_entries as F[];
    }
    
    abstract get_document_by_sha(shas: ArrayBuffer[], collectionName: string) : Promise<(FeatherDocumentEntry | undefined)[]>;
    abstract get_document_by_uuid(uuid: Uint8Array, collectionName: string) : Promise<FeatherDocumentEntry | undefined>;
    abstract bulk_get_document_by_uuid(uuids: Uint8Array[], collectionName: string) : Promise<FeatherDocumentEntry[]>;

    abstract search_by_title(title: string, collectionName: string): Promise<FeatherDocumentEntry[]>;
    
    abstract insert_internal(documents: FeatherDocumentEntry[], collectionName: string) : Promise<Uint8Array[]>;
    abstract delete_internal(uuids: Uint8Array[], collectionName: string) : Promise<Uint8Array[]>;
}   

export function SHAToHexString(array : Uint8Array): string {
    return Array.from(array)
      .map(byte => byte.toString(16).padStart(2, '0'))
      .join('');
}