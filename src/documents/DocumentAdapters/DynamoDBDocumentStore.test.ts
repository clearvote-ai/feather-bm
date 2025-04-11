import test_feather_docs from "../../test_data/test_feather_docs.json";
import { FeatherDocument, IngestionDocument } from "../../documents/FeatherDocumentStore.d";
import { parse, stringify } from "uuid";
import { DynamoDBDocumentStore } from "./DynamoDBDocumentStore";
import { CreateTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

const local_dynamo_client = new DynamoDBClient({
    region: "us-west-2",
    endpoint: "http://localhost:8000",
    credentials: {
        accessKeyId : "fakeKey",
        secretAccessKey: "fakeSecret"
    }
});

describe('DynamoDBDocumentStore', () => {
    test('createTable', async () => {
            const client = DynamoDBDocumentClient.from(local_dynamo_client);
            const table = await client.send( new CreateTableCommand( {
                TableName: "FeatherDocumentTable",
                //partition key "indexName" and sort key "sortkey"
                KeySchema: [
                    { AttributeName: "pk", KeyType: "HASH" },
                    { AttributeName: "id", KeyType: "RANGE" }
                ],
                AttributeDefinitions: [
                    { AttributeName: "pk", AttributeType: "S" },
                    { AttributeName: "id", AttributeType: "B" },
                    { AttributeName: "sha", AttributeType: "B" },
                    { AttributeName: "t", AttributeType: "S" }
                ],
                GlobalSecondaryIndexes: [
                    {
                        IndexName: "SHAIndex",
                        KeySchema: [
                            { AttributeName: "pk", KeyType: "HASH" },
                            { AttributeName: "sha", KeyType: "RANGE" }
                        ],
                        Projection: {
                            ProjectionType: "KEYS_ONLY"
                        },
                    },
                    {
                        IndexName: "TitleIndex",
                        KeySchema: [
                            { AttributeName: "pk", KeyType: "HASH" },
                            { AttributeName: "t", KeyType: "RANGE" }
                        ],
                        Projection: {
                            //TODO: shrink the fields down to necessary only for search results
                            ProjectionType: "ALL"
                        },
                    }
                ],
                //TODO: swap billing mode and test with provisioned and retries
                BillingMode: "PAY_PER_REQUEST"
            }));
            
        });
    
    test('insert', async () => {
        const docs = test_feather_docs as unknown as FeatherDocument[];

        const first_doc_raw = docs[0];
        const first_doc_uuid = first_doc_raw.id;
        const first_doc_uuid_bytes = parse(first_doc_uuid);
        const stringified_uuid = stringify(first_doc_uuid_bytes);

        expect(stringified_uuid).toBe(first_doc_raw.id);

        const store = new DynamoDBDocumentStore(local_dynamo_client, "FeatherDocumentTable", false);

        await store.insert(docs, "test_index");

        //"01857a13-dc00-7b19-86a7-ba83ceee585e"
        const first_doc = await store.get("01857a13-dc00-7b19-86a7-ba83ceee585e", "test_index");

        expect(first_doc).toBeDefined();
        expect(first_doc?.id).toEqual(stringified_uuid);
        expect(first_doc?.title).toEqual(first_doc_raw.title);
        expect(first_doc?.text).toEqual(first_doc_raw.text);
        expect(first_doc?.published).toEqual(first_doc_raw.published);
        expect(first_doc?.pk).toEqual("test_index");

        console.log(first_doc);
    },10000);

    test('insert_compressed', async () => {
        const docs = test_feather_docs as unknown as FeatherDocument[];

        const first_doc_raw = docs[0];
        const first_doc_uuid = first_doc_raw.id;
        const first_doc_uuid_bytes = parse(first_doc_uuid);
        const stringified_uuid = stringify(first_doc_uuid_bytes);

        expect(stringified_uuid).toBe(first_doc_raw.id);

        const store = new DynamoDBDocumentStore(local_dynamo_client, "FeatherDocumentTable", true);

        await store.insert(docs.slice(0,100), "test_index");

        //"01857a13-dc00-7b19-86a7-ba83ceee585e"
        const first_doc = await store.get("01857a13-dc00-7b19-86a7-ba83ceee585e", "test_index");

        expect(first_doc).toBeDefined();
        expect(first_doc?.id).toEqual(stringified_uuid);
        expect(first_doc?.title).toEqual(first_doc_raw.title);
        expect(first_doc?.text).toEqual(first_doc_raw.text);
        expect(first_doc?.published).toEqual(first_doc_raw.published);
        expect(first_doc?.pk).toEqual("test_index");

        console.log(first_doc);
    },10000);

    test('search_by_title', async () => {
        const docs = test_feather_docs as unknown as FeatherDocument[];

        const first_doc_raw = docs[0];
        const first_doc_uuid = first_doc_raw.id;
        const first_doc_uuid_bytes = parse(first_doc_uuid);
        const stringified_uuid = stringify(first_doc_uuid_bytes);

        expect(stringified_uuid).toBe(first_doc_raw.id);

        const store = new DynamoDBDocumentStore(local_dynamo_client, "FeatherDocumentTable", false);

        const search_title = first_doc_raw.title?.slice(0,15) as string;

        const search_results = await store.search_by_title(search_title, "test_index");
        expect(search_results.length).toBeGreaterThan(0);
        expect(search_results[0].t).toEqual(first_doc_raw.title);
    });
})