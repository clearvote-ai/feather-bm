import { FeatherDocumentEntry } from "../FeatherDocumentStore.d";
import { FeatherDocumentStore } from "../FeatherDocumentStore";
import { BatchWriteCommand, DynamoDBDocumentClient, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { DYNAMO_DB_MAX_BATCH_SIZE } from "../../search/SearchAdapters/DynamoDBIndex";

export class DynamoDBDocumentStore extends FeatherDocumentStore 
{

    client: DynamoDBDocumentClient;
    tableName: string;
        
    constructor(client: DynamoDBDocumentClient, tableName: string, indexName: string)
    {
        super(indexName);
        this.tableName = tableName;
        this.client = client;
    }

    get_document_by_sha(shas: ArrayBuffer[]): Promise<(FeatherDocumentEntry | null)[]> {
        throw new Error("Method not implemented.");
    }

    get_document_by_uuid(uuid: Uint8Array): Promise<FeatherDocumentEntry | null> {
        throw new Error("Method not implemented.");
    }

    search_by_title(title: string): Promise<FeatherDocumentEntry[]> {
        throw new Error("Method not implemented.");
    }

    async insert_internal(documents: FeatherDocumentEntry[]): Promise<Uint8Array[]> 
    {
        return await this.putDynamoDBDocumentEntryBatch(this.client, this.tableName, documents);
    }

    async delete_internal(uuids: Uint8Array[]): Promise<Uint8Array[]> {
        return await this.deleteDynamoDBEntryBatch(this.client, this.tableName, this.indexName, uuids);
    }

    async getDocumentByUUID(client: DynamoDBDocumentClient, table_name: string, index_name: string, uuid: Uint8Array): Promise<FeatherDocumentEntry | null>
    {
        const params = {
            TableName: table_name,
            Key: {
                pk: index_name,
                id: uuid
            }
        };
        
        try {
            const data = await client.send(new GetCommand(params));
            if(data.Item === undefined) return null;
            return data.Item as FeatherDocumentEntry;
        } catch (error) {
            console.error("Error getting document by uuid:", error);
        }

        return null;
    }

    async deleteDynamoDBEntryBatch(client: DynamoDBDocumentClient, table_name: string, index_name: string, uuids: Uint8Array[]): Promise<Uint8Array[]> 
    {
        const entries : Uint8Array[] = [];

        for (let i = 0; i < uuids.length; i += DYNAMO_DB_MAX_BATCH_SIZE) 
        {
            const batch = uuids.slice(i, i + DYNAMO_DB_MAX_BATCH_SIZE);
            const deleteRequests = batch.map(uuid => ({
                DeleteRequest: {
                    Key: {
                        pk: index_name,
                        id: uuid,
                    }
                }
            }));
            // Create the params for the BatchWriteCommand
            const params = {
                RequestItems: {
                    [table_name]: deleteRequests
                }
            };

            try {
                await client.send(new BatchWriteCommand(params));
                entries.push(...batch);
            }
            catch (error) {
                console.error("Error deleting entries from DynamoDB", { error });
            }
        }

        return entries;
    }

    async putDynamoDBDocumentEntryBatch(client: DynamoDBDocumentClient, table_name: string, documents: FeatherDocumentEntry[]): Promise<Uint8Array[]>
    {
        const entries : Uint8Array[] = [];

        for (let i = 0; i < documents.length; i += DYNAMO_DB_MAX_BATCH_SIZE) 
        {
            const batch = documents.slice(i, i + DYNAMO_DB_MAX_BATCH_SIZE);
            const putRequests = batch.map(entry => ({
                PutRequest: {
                    Item: entry
                }
            }));
            // Create the params for the BatchWriteCommand
            const params = {
                RequestItems: {
                    [table_name]: putRequests
                }
            };

            try {
                await client.send(new BatchWriteCommand(params));
                entries.push(...batch.map(entry => entry.id));
            }
            catch (error) {
                console.error("Error putting entries into DynamoDB", { error });
            }
        }

        return entries;
    }

    async getDocumentBySHA(client: DynamoDBDocumentClient, table_name: string, sha: ArrayBuffer): Promise<FeatherDocumentEntry | null>
    {
        const params = {
            TableName: table_name,
            IndexName: "sha_index",
            KeyConditionExpression: "sha = :sha",
            ExpressionAttributeValues: {
                ":sha": { B: new Uint8Array(sha) }
            }
        };
        
        try {
            const data = await client.send(new QueryCommand(params));
            if(data.Items === undefined) return null;
            return data.Items[0] as FeatherDocumentEntry;
        } catch (error) {
            console.error("Error getting document by sha:", error);
        }

        return null;
    }
    
}