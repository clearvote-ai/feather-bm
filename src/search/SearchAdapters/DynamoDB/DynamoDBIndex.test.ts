import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { DynamoDBIndex } from "./DynamoDBIndex";
import { CreateTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { IndexedDocument } from "../../../FeatherTypes";
import test_docs from "../../../test_data/test_docs.json";

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
        const docs = test_docs as IndexedDocument[];

        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "FeatherIndex", "test_index");

        await index.insert(docs);
    }, 1000000);


    test('query', async () => {
        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "FeatherIndex", "test_index");

        const scores = await index.query("franchise tax");

        const top_score = scores[0];

        console.log("Top Score: ", top_score);

        const docs = test_docs as IndexedDocument[];

        const top_doc = docs.find(doc => doc.uuidv7 === top_score.id);

        
        console.log("Top Document: ", top_doc);

    }, 100000);

    test('delete', async () => {
        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "FeatherIndex", "test_index");

        const scores = await index.query("franchise tax");

        const full_docs = test_docs as IndexedDocument[];

        const scored_docs = scores.map(score => full_docs.find(doc => doc.uuidv7 === score.id));

        await index.delete(scored_docs as IndexedDocument[]);

        const new_scores = await index.query("franchise tax");

        expect(new_scores.length).toBe(0);
        
    }, 100000);
});