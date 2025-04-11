import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentStore } from "./documents/DocumentAdapters/DynamoDBDocumentStore";
import { FeatherDocumentStore } from "./documents/FeatherDocumentStore";
import { FeatherBMIndex } from "./search/FeatherBMIndex";
import { DynamoDBIndex } from "./search/SearchAdapters/DynamoDBIndex";
import { FeatherDocument, IngestionDocument } from "./documents/FeatherDocumentStore.d";
import { HashIndex } from "./search/SearchAdapters/HashIndex";
import { HashDocumentStore } from "./documents/DocumentAdapters/HashDocumentStore";



export class FeatherBM<T extends FeatherBMIndex,K extends FeatherDocumentStore>
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

    static async fromHashMemory(docs: FeatherDocument[], index_name: string): Promise<FeatherBM<HashIndex, HashDocumentStore>>
    {
        const index = await HashIndex.from(docs, index_name);
        const storage = new HashDocumentStore();
        return new FeatherBM<HashIndex,HashDocumentStore>(index, storage);
    }

    async insert(documents: IngestionDocument | IngestionDocument[], indexName: string): Promise<void>
    {
        if (!Array.isArray(documents)) {
            documents = [documents];
        }

        const featherDocuments = documents.map((doc) => this.ingestDocument(doc));

        await this.storage.insert(featherDocuments, indexName);
        await this.index.insert(featherDocuments, indexName);
    }

    async delete(ids: Uint8Array | Uint8Array[], indexName: string): Promise<void>
    {
        if (!Array.isArray(ids)) {
            ids = [ids];
        }

        const docs = await this.storage.bulk_get(ids, indexName);

        await this.storage.delete(ids, indexName);
        await this.index.delete(docs, indexName);
    }

    ingestDocument(doc: IngestionDocument): FeatherDocument
    {
        const { uuidv7, title, text, sha } = doc;
        //TODO: finish the rules for ingestion
        //if the uuidv7 is not set

        const document: FeatherDocument = {
            id: uuidv7,
            title: title,
            text: text,
            sha: sha
        };
        return document;
    }
}