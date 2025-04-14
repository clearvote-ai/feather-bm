# 🪶Feather BM 
100% serverless BM25 index + search 

Built with DynamoDB in mind but supports DB adapters.

⚠️ Probably not production ready yet.

## Tradeoffs and Usecases
**TL;DR A: Feather-BM is extremely cheap and scalable, but you have no flexibility in how you set the BM25 (k1, b, avgDL) params once you create a collection**

You'll need to estimate the average document length of your collection and know the BM25 search parameters you want to use before you create the collection.

#### Why would I use this?
- You have a large set of document collections that each have a small number of documents (less than 100,000).

   Maybe you're a Sass company that needs to manage a set of documents for each of your customers.

- Your data is basically immutable.

   Your users/data sources are publishing documents to be indexed not to be edited.

- The average reads and writes for your collections are very low and would therefore be cost ineffective to use a more expensive solution like ElasticSearch.


## Features
#### Virtually Unlimited Collections
  Collections are logical objects split across multiple partition keys designed to efficiently scale horizontally (across multiple nodes).
#### Search by keyword (Globally)
```typescript
await bm.query(
    "Arkansas Citizens First Responder Safety Enhancement Fund", //query
    "my_first_collection_name" //collection name
);
```
#### Search by keyword (Within a specified time range)
```typescript
await bm.query(
    "Arkansas Citizens First Responder Safety Enhancement Fund", //query
    "my_first_collection_name", //collection name
    "2023-01-01T00:00:00.000Z", //start time
    "2024-01-01T23:59:59.999Z" //end time
);
```
#### Search by title (begins_with/exact match)
```typescript
await bm.searchByTitle(
    "House Bill 1070", //title exact match or text title begins with
    "my_first_collection_name" //collection name
);
```
#### List documents in chronological created_at order
```typescript
await bm.list(
    "my_first_collection_name", //collection name
    10, //number of documents to return
    "2023-01-01T00:00:00.000Z", //start time
    "2024-01-01T23:59:59.999Z" //end time
);
```
#### Check if a text body already exists in a collection
```typescript
await bm.exists(
    "Arkansas Citizens First Responder Safety Enhancement Fund", //text body
    "my_first_collection_name" //collection name
);
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

## Testing
#### Unit Tests
Feather-BM Unit tests use an in memory bootleg of DynamoDB functionality. 

#### Integration Tests
Feather-BM Integration tests use a local DynamoDB instance.
To create the local DynamoDB instance:
1. Install Docker Desktop (or Docker)
2. Use the docker-compose file in the repo to create the local DynamoDB instance.
```bash
docker-compose up -d docker/dynamo_db_local/docker-compose.yml
```
## License
MIT
```
Copyright 2025 CLEARVOTE LLC.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

