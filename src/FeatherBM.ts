import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentStore } from "./documents/DocumentAdapters/DynamoDBDocumentStore";
import { FeatherDocumentStore } from "./documents/FeatherDocumentStore";
import { FeatherBMIndex } from "./search/FeatherBMIndex";
import { DynamoDBIndex } from "./search/SearchAdapters/DynamoDBIndex";



export class FeatherBM<T extends FeatherBMIndex,K extends FeatherDocumentStore>
{
    index : T;
    storage: K;

    constructor(index: T, storage: K)
    {
        this.index = index;
        this.storage = storage;
    }

    static async fromDynamoDB(client: DynamoDBClient): Promise<FeatherBM<DynamoDBIndex, DynamoDBDocumentStore>>
    {
        const index = await DynamoDBIndex.from(client, "test_table");
        const storage = new DynamoDBDocumentStore(client, "test_table", "test_index");
        return new FeatherBM<DynamoDBIndex,DynamoDBDocumentStore>(index, storage);
    }


}