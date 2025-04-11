import { FeatherDocumentEntry } from "../FeatherDocumentStore.d";
import { FeatherDocumentStore } from "../FeatherDocumentStore";
import { BatchWriteCommand, DynamoDBDocumentClient, GetCommand, QueryCommand, BatchGetCommand, BatchGetCommandInput } from "@aws-sdk/lib-dynamodb";
import { DYNAMO_DB_MAX_BATCH_SIZE } from "../../search/SearchAdapters/DynamoDBIndex";

export class DynamoDBDocumentStore extends FeatherDocumentStore 
{
    client: DynamoDBDocumentClient;
    tableName: string;
        
    constructor(client: DynamoDBDocumentClient, tableName: string, enableCompression: boolean = true)
    {
        super(enableCompression);
        this.tableName = tableName;
        this.client = client;
    }

    get_document_by_sha(shas: ArrayBuffer[], indexName: string): Promise<(FeatherDocumentEntry | undefined)[]> {
        return Promise.all(shas.map(sha => this.getDocumentBySHA(this.client, this.tableName, indexName, sha)));
    }

    get_document_by_uuid(uuid: Uint8Array, indexName: string): Promise<FeatherDocumentEntry | undefined> {
        return this.getDocumentByUUID(this.client, this.tableName, indexName, uuid);
    }

    async search_by_title(title: string, indexName: string): Promise<FeatherDocumentEntry[]> {
        //search for titles that begin with or equal the given title
        //eg. title:"fran" should match "franchise tax"

        const params = {
            TableName: this.tableName,
            IndexName: "TitleIndex",
            KeyConditionExpression: "pk = :pk and begins_with(t, :t)",
            ExpressionAttributeValues: {
                ":t": title,
                ":pk": indexName
            },
        };

        try {
            const data = await this.client.send(new QueryCommand(params));
            if(data.Items === undefined) return [];
            return data.Items as FeatherDocumentEntry[];
        }catch (error) {
            console.error("Error getting document by title:", error);
            return [];
        }

    }

    async insert_internal(documents: FeatherDocumentEntry[]): Promise<Uint8Array[]> 
    {
        return await this.putDynamoDBDocumentEntryBatch(this.client, this.tableName, documents);
    }

    async delete_internal(uuids: Uint8Array[], indexName: string): Promise<Uint8Array[]> {
        return await this.deleteDynamoDBEntryBatch(this.client, this.tableName, indexName, uuids);
    }

    async getDocumentByUUID(client: DynamoDBDocumentClient, table_name: string, indexName: string, uuid: Uint8Array): Promise<FeatherDocumentEntry | undefined>
    {
        const params = {
            TableName: table_name,
            Key: {
                pk: indexName,
                id: uuid
            }
        };
        
        try {
            const data = await client.send(new GetCommand(params));
            if(data.Item === undefined) return undefined;
            return data.Item as FeatherDocumentEntry;
        } catch (error) {
            console.error("Error getting document by uuid:", error);
        }

        return undefined;
    }

    async bulk_get_document_by_uuid(uuids: Uint8Array[], indexName: string): Promise<FeatherDocumentEntry[]> {
        const batch_params: BatchGetCommandInput = {
            RequestItems: {
                [this.tableName]: {
                    Keys: uuids.map(uuid => ({
                        pk: indexName,
                        id: uuid
                    }))
                }
            }
        };

        try {
            const data = await this.client.send(new BatchGetCommand(batch_params));
            if(data.Responses === undefined) return [];
            return data.Responses[this.tableName] as FeatherDocumentEntry[];
        } catch (error) {
            console.error("Error getting documents by uuid:", error);
            return [];
        }
    }

    async deleteDynamoDBEntryBatch(client: DynamoDBDocumentClient, table_name: string, indexName: string, uuids: Uint8Array[]): Promise<Uint8Array[]> 
    {
        const entries : Uint8Array[] = [];

        for (let i = 0; i < uuids.length; i += DYNAMO_DB_MAX_BATCH_SIZE) 
        {
            const batch = uuids.slice(i, i + DYNAMO_DB_MAX_BATCH_SIZE);
            const deleteRequests = batch.map(uuid => ({
                DeleteRequest: {
                    Key: {
                        pk: indexName,
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

    //TODO: convert this to a batch call
    async getDocumentBySHA(client: DynamoDBDocumentClient, table_name: string, indexName: string, sha: ArrayBuffer): Promise<FeatherDocumentEntry | undefined>
    {
        //sha_index pk is the indexName
        //and the sha is the sort key
        const params = {
            TableName: table_name,
            IndexName: "SHAIndex",
            KeyConditionExpression: "pk = :pk and sha = :sha",
            ExpressionAttributeValues: {
                ":sha": new Uint8Array(sha),
                ":pk": indexName
            }
        };
        
        try {
            const data = await client.send(new QueryCommand(params));
            if(data.Items === undefined) return undefined;
            return data.Items[0] as FeatherDocumentEntry;
        } catch (error) {
            console.error("Error getting document by sha:", error);
        }

        return undefined;
    }

    async getDocumentByTitle(client: DynamoDBDocumentClient, table_name: string, title: string): Promise<FeatherDocumentEntry | null>
    {
        
        const params = {
            TableName: table_name,
            IndexName: "title_index",
            //use begins with to match the title
            KeyConditionExpression: "title = :title",
            ExpressionAttributeValues: {
                ":title": { S: title }
            }
        };
        
        try {
            const data = await client.send(new QueryCommand(params));
            if(data.Items === undefined) return null;
            return data.Items[0] as FeatherDocumentEntry;
        } catch (error) {
            console.error("Error getting document by title:", error);
        }

        return null;
    }
    
}