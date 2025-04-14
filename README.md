# 🪶Feather BM 
100% serverless BM25 (ish) index + search
## Features
#### Virtually Unlimited Indexes and Collections
  Collections are logical objects split across multiple partition keys designed to efficiently scale horizontally (across multiple nodes).
#### Search by keyword 
```typescript
const docs : FeatherDocuement[] = await bm.query("Arkansas Citizens First Responder Safety Enhancement Fund", "my_first_collection_name");
```
#### Search by title (begins_with/exact match)
```typescript
const docs : FeatherDocuement[] = await bm.searchByTitle("House Bill 1070", "my_first_collection_name");
```
#### List documents in chronological created_at order
```typescript
const docs : FeatherDocuement[] = await bm.list("my_first_collection_name", 10, "2023-01-01T00:00:00.000Z");
```
#### Check if a text body already exists in a collection
```typescript
const exists : boolean = await bm.exists("Arkansas Citizens First Responder Safety Enhancement Fund", "my_first_collection_name");
```

## Installation
1. `npm install feather-bm`
2. Create the tables in DynamoDB
    1. Create a table called `ANY_TABLE_NAME_index` with the following attributes:
        - `pk` (string) - The partition key for the index. 
        - `id` (binary) - The uuidv7 of the document.
    2. Create a GSI on the `ANY_TABLE_NAME_index` table with the following attributes:
        - `pk` (string) - The partition key for the index.
        - `tf` (number) - The static BM25 term frequency component.
    3. Create a table called `ANY_TABLE_NAME_documents` with the following attributes:
        - `pk` (string) - The partition key for the meta data. 
        - `id` (binary) - The uuidv7 of the document.
    4. Create a GSI on the `ANY_TABLE_NAME_documents` table with the following attributes:
        - `pk` (string) - The partition key for the meta data.
        - `t` (string) - The title of the document.
3. Create AWS Client Credentials with access to the tables.
 ```typescript
 const dynamo_client = new DynamoDBClient({
     region: "us-west-2",
     credentials: {
         accessKeyId : "fakeKey",
         secretAccessKey: "fakeSecret"
     }
 });
 ```



## Usage
```typescript
import { FeatherBM } from 'feather-bm';

const bm = await FeatherBM.fromDynamoDB(dynamo_client, "ANY_TABLE_NAME");

await bm.insert(docs, "my_first_collection_name");

const title_result = await bm.searchByTitle("House Bill 1070", "my_first_collection_name");

const query_result = await bm.query("Arkansas Citizens First Responder Safety Enhancement Fund", "my_first_collection_name");

```

