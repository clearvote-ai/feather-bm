import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { DynamoDBIndex } from "./DynamoDBIndex";
import { CreateTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { FeatherDocument, IngestionDocument } from "../../documents/FeatherDocumentStore.d";
import test_feather_docs from "../../test_data/test_feather_docs.json";

const local_dynamo_client = new DynamoDBClient({
    region: "us-west-2",
    endpoint: "http://localhost:8000",
    credentials: {
        accessKeyId : "fakeKey",
        secretAccessKey: "fakeSecret"
    }
});


describe('DynamoDB', () => {

    test('createTable', async () => {
        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const table = await client.send( new CreateTableCommand( {
            TableName: "FeatherIndex",
            //partition key "indexName" and sort key "sortkey"
            KeySchema: [
                { AttributeName: "pk", KeyType: "HASH" },
                { AttributeName: "id", KeyType: "RANGE" }
            ],
            AttributeDefinitions: [
                { AttributeName: "pk", AttributeType: "S" },
                { AttributeName: "id", AttributeType: "B" },
                { AttributeName: "tf", AttributeType: "N" }
            ],
            GlobalSecondaryIndexes: [
                {
                    IndexName: "GlobalIndex",
                    KeySchema: [
                        { AttributeName: "pk", KeyType: "HASH" },
                        { AttributeName: "tf", KeyType: "RANGE" }
                    ],
                    Projection: {
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

        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "FeatherIndex");

        await index.insert(docs, "test_index");
    }, 1000000);


    test('local_query', async () => {
        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "FeatherIndex");

        const scores = await index.query("franchise tax","test_index", false, 100);

        const top_score = scores[0];

        console.log("Top Score: ", top_score);

        const docs = test_feather_docs as unknown as FeatherDocument[];

        const top_doc = docs.find(doc => doc.id === top_score.id);

        console.log("Top Document: ", top_doc);

    }, 100000);

    test('global_query', async () => {
        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "FeatherIndex");

        const scores = await index.query("franchise tax","test_index", true, 10);

        //expect(scores.length).toBe(10);

        const top_score = scores[0];

        console.log("Top Score: ", top_score);

        const docs = test_feather_docs as unknown as FeatherDocument[];

        const top_doc = docs.find(doc => doc.id === top_score.id);

        console.log("Top Document: ", top_doc);

    }, 100000);

    test('delete', async () => {
        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "FeatherIndex");

        const scores = await index.query("franchise tax","test_index");

        const full_docs = test_feather_docs as unknown as FeatherDocument[];

        const scored_docs = scores.map(score => full_docs.find(doc => doc.id === score.id));

        await index.delete(scored_docs as FeatherDocument[], "test_index");

        const new_scores = await index.query("franchise tax","test_index");

        expect(new_scores.length).toBe(0);
        
    }, 100000);
});