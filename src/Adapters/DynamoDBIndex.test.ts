import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { IndexedDocument } from "../BM25/InvertedIndex";
import { computeBM25ScoresConcurrent } from "../BM25/Search";
import test_docs from "../BM25/test_data/arkansas_2023.json";
import { DynamoDBIndex } from "./DynamoDBIndex";
import { CreateTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";

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
            TableName: "test_table",
            //partition key "index_name" and sort key "sortkey"
            KeySchema: [
                { AttributeName: "index_name", KeyType: "HASH" },
                { AttributeName: "sortkey", KeyType: "RANGE" }
            ],
            AttributeDefinitions: [
                { AttributeName: "index_name", AttributeType: "S" },
                { AttributeName: "sortkey", AttributeType: "S" }
            ],
            BillingMode: "PAY_PER_REQUEST"
        }));
    });

    test('insert', async () => {
        const docs = test_docs as IndexedDocument[];

        //pick a random document from the first 1000
        const test_doc = docs[Math.floor(Math.random() * 1000)];

        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "test_table", "test_index");

        await index.insert_batch(docs);
    }, 100000);


    test('query', async () => {
        const client = DynamoDBDocumentClient.from(local_dynamo_client);
        const index = await DynamoDBIndex.from(client, "test_table", "test_index");

        const scores = await index.query("franchise tax");

        console.log(scores);
    }, 100000);
});