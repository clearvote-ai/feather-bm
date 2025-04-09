import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { DynamoDBIndex } from "./DynamoDBIndex";
import { CreateTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { IngestionDocument } from "../../documents/FeatherDocumentStore.d";
import test_docs from "../../test_data/test_docs.json";

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
                { AttributeName: "id", AttributeType: "B" }
            ],
            //TODO: swap billing mode and test with provisioned and retries
            BillingMode: "PAY_PER_REQUEST"
        }));
    });

    test('insert', async () => {
        const docs = test_docs as IngestionDocument[];

        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "FeatherIndex");

        await index.insert(docs, "test_index");
    }, 1000000);


    test('query', async () => {
        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "FeatherIndex");

        const scores = await index.query("franchise tax","test_index");

        const top_score = scores[0];

        console.log("Top Score: ", top_score);

        const docs = test_docs as IngestionDocument[];

        const top_doc = docs.find(doc => doc.uuidv7 === top_score.id);

        
        console.log("Top Document: ", top_doc);

    }, 100000);

    test('delete', async () => {
        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "FeatherIndex");

        const scores = await index.query("franchise tax","test_index");

        const full_docs = test_docs as IngestionDocument[];

        const scored_docs = scores.map(score => full_docs.find(doc => doc.uuidv7 === score.id));

        await index.delete(scored_docs as IngestionDocument[], "test_index");

        const new_scores = await index.query("franchise tax","test_index");

        expect(new_scores.length).toBe(0);
        
    }, 100000);
});