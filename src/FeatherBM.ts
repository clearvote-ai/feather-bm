import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentStore } from "./documents/DocumentAdapters/DynamoDBDocumentStore";
import { FeatherDocumentStore } from "./documents/FeatherDocumentStore";
import { FeatherBMIndex } from "./search/FeatherBMIndex";
import { DynamoDBIndex } from "./search/SearchAdapters/DynamoDBIndex";
import { FeatherDocument, IngestionDocument } from "./documents/FeatherDocumentStore.d";
import { HashIndex } from "./search/SearchAdapters/HashIndex";
import { HashDocumentStore } from "./documents/DocumentAdapters/HashDocumentStore";
import { uuidv7 } from "uuidv7";
import { parse, stringify } from "uuid";

import { DateTime } from "luxon";
import { sha256 } from "js-sha256";

export class FeatherBM<T extends FeatherBMIndex, K extends FeatherDocumentStore>
{
    index : T;
    storage: K;

    constructor(index: T, storage: K)
    {
        this.index = index;
        this.storage = storage;
    }

    static async fromDynamoDB(client: DynamoDBClient, tableBaseName: string): Promise<FeatherBM<DynamoDBIndex, DynamoDBDocumentStore>>
    {
        const index = await DynamoDBIndex.from(client, tableBaseName + "_index");
        const storage = new DynamoDBDocumentStore(client, tableBaseName + "_documents");
        return new FeatherBM<DynamoDBIndex,DynamoDBDocumentStore>(index, storage);
    }

    static async fromInMemory(enable_compression: boolean): Promise<FeatherBM<HashIndex, HashDocumentStore>>
    {
        const index = new HashIndex();
        const storage = new HashDocumentStore(enable_compression);
        return new FeatherBM<HashIndex,HashDocumentStore>(index, storage);
    }

    async get<F extends FeatherDocument>(uuid: string, collectionName: string): Promise<F | undefined>
    {
        return await this.storage.get<F>(uuid, collectionName);
    }

    async searchByTitle<F extends FeatherDocument>(query: string, collectionName: string): Promise<F[]>
    {
        return await this.storage.searchByTitle<F>(query, collectionName);
    }

    async query<F extends FeatherDocument>(query: string, collectionName: string, global: boolean = true, maxResults: number = 100): Promise<F[]>
    {
        const scores = await this.index.query(query, collectionName, global, maxResults);
        const ids = scores.map((score) => score.id);
        const documents = await this.storage.bulk_get(ids, collectionName);
        const filtered_documents = documents.filter((doc) => doc !== undefined) as F[];
        return filtered_documents;
    }

    async insert(documents: IngestionDocument | IngestionDocument[], collectionName: string): Promise<void>
    {
        if (!Array.isArray(documents)) {
            documents = [documents];
        }

        const featherDocuments = documents.map((doc) => this.ingestDocument(doc, collectionName));

        await this.storage.insert(featherDocuments, collectionName);
        await this.index.insert(featherDocuments, collectionName);
    }

    async delete(ids: string | string[], collectionName: string): Promise<void>
    {
        if (!Array.isArray(ids)) {
            ids = [ids];
        }

        const docs = await this.storage.bulk_get(ids, collectionName);

        await this.storage.delete(ids, collectionName);
        await this.index.delete(docs, collectionName);
    }

    private ingestDocument<D extends IngestionDocument, F extends FeatherDocument>(doc: D, collectionName: string): F
    {
        const { title, text, published, iso8601 } = doc;

        //separate out the custom fields from the document
        const custom_field_keys = Object.keys(doc).filter((key) => !["title", "text", "published", "iso8601"].includes(key));
        const custom_fields = custom_field_keys.reduce((acc, key) => {
            acc[key] = (doc as Record<string, any>)[key];
            return acc;
        }, {} as Record<string, any>);

        const uuid = uuidv7();

        //IF dateTime exists pack it into the first 6 bytes of the uuidv7 otherwise use the uuidv7 as is
        const id = iso8601 ? packUUIDWithDateTime(uuid, iso8601) : uuid;
        
        const sha = sha256(text);

        return {
            pk: collectionName,
            id: id,
            title: title,
            text: text,
            published: published,
            sha: sha,
            //copy custom fields from the document
            ...custom_fields,
        } as F;
    };
}


function packUUIDWithDateTime(uuid: string, iso8601: string) : string
{
    const uuid_bytes = parse(uuid);

    const dateTimeBytes = DateTime.fromISO(iso8601).toMillis();
    uuid_bytes[0] = (dateTimeBytes >> 8) & 0xFF;
    uuid_bytes[1] = (dateTimeBytes >> 16) & 0xFF;
    uuid_bytes[2] = (dateTimeBytes >> 24) & 0xFF;
    uuid_bytes[3] = (dateTimeBytes >> 32) & 0xFF;
    uuid_bytes[4] = (dateTimeBytes >> 40) & 0xFF;
    uuid_bytes[5] = (dateTimeBytes >> 48) & 0xFF;

    return stringify(uuid_bytes);
}