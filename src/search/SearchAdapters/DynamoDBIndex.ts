import { BatchWriteCommand, DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { GlobalStatisticsEntry, InverseDocumentFrequencyEntry, TermFrequencyEntry } from "../FeatherBMIndex.d";
import { FeatherBMIndex, UUID_000 } from "../FeatherBMIndex";

export const DYNAMO_DB_MAX_BATCH_SIZE = 25;
export class DynamoDBIndex extends FeatherBMIndex
{
    
    getEntriesGlobal(token: string, indexName: string, max_results?: number): Promise<{ idf_entry: InverseDocumentFrequencyEntry; tf_entries: TermFrequencyEntry[]; }> {
        throw new Error("Method not implemented.");
    }
    
    client: DynamoDBDocumentClient;
    tableName: string;
    
    constructor(client: DynamoDBDocumentClient, tableName: string)
    {
        super(0, 0);
        this.tableName = tableName;
        this.client = client;
    }

    async getEntries(token: string, indexName: string, max_results?: number): Promise<{ idf_entry: InverseDocumentFrequencyEntry; tf_entries: TermFrequencyEntry[]; }> {
        try {
            const idf_entry = await this.getInverseDocumentFrequencyEntry(token, indexName);
            const tf_entries = await this.getTermFrequencyEntries(token, indexName);
            return { idf_entry: { pk: `${indexName}#${token}`, id: UUID_000, idf: idf_entry }, tf_entries };
        } catch (error) {
            console.error("Error getting entries:", error);
            throw new Error("Failed to retrieve entries from DynamoDB.");
        }
    }

    async insert_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[]): Promise<void> {
        //TODO: convert the underlying call to an atomic add or subtraction transaction so its thread safe FOR IDF
        await this.putDynamoDBEntryBatch(idf_entries);
        await this.putDynamoDBEntryBatch(tf_entries);
    }

    async delete_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[]): Promise<void> {
        //TODO: convert the underlying call to an atomic add or subtraction transaction so its thread safe FOR IDF
        await this.deleteDynamoDBEntryBatch(idf_entries);
        await this.deleteDynamoDBEntryBatch(tf_entries);
    }

    async update_global_entry_internal(global_stats: GlobalStatisticsEntry): Promise<void> {
        //TODO: convert the underlying call to an atomic add or subtraction transaction so its thread safe
        await this.updateGlobalStatsEntry(global_stats);
    }

    static async from(client: DynamoDBDocumentClient, tableName: string): Promise<DynamoDBIndex>
    {
        //const global_entry = await DynamoDBIndex.getGlobalStatsEntry(client, tableName, indexName);
        const index = new DynamoDBIndex(client, tableName);
        return index;
    }

    async get_global_entry_internal(indexName: string): Promise<GlobalStatisticsEntry> {
        const params = {
            TableName: this.tableName,
            Key: {
                pk: `${indexName}#global_stats`,
                id: UUID_000
            }
        };
        
        try {
            const data = await this.client.send(new GetCommand(params));
            if(data.Item === undefined) throw new Error("Global stats entry not found");
            return data.Item as GlobalStatisticsEntry;
        } catch (error) {
            console.error("Error reading global stats entry:", error);
        }

        return { pk: `${indexName}#global_stats`, id: UUID_000, totalDocumentLength: 0, documentCount: 0}; // return a default value if not found
    }

    async getTermFrequencyEntries(token: string, indexName: string): Promise<TermFrequencyEntry[]> {
        const params = {
            TableName: this.tableName,
            KeyConditionExpression: "pk = :pk",
            ExpressionAttributeValues: {
                ":pk": `${indexName}#${token}`,
            },
        };

        try {
            const data = await this.client.send(new QueryCommand(params));
            if(data.Items === undefined) return [];

            // Filter out the UUID_000 entries (which are not term frequency entries)
            const items = data.Items.filter(item => item.idf === undefined);
            if (items.length === 0) return [];
            return items as TermFrequencyEntry[];
        } catch (error) {
            console.error("Error getting index entry:", error);
        }

        return [];
    }

    async getInverseDocumentFrequencyEntry(token: string, indexName: string): Promise<number> {  
        const params = {
            TableName: this.tableName,
            Key: {
                pk: `${indexName}#${token}`,
                id: UUID_000 // placeholder for idf entry
            }
        };
        
        try {
            const data = await this.client.send(new GetCommand(params));
            if(data.Item === undefined) return 0;
            return data.Item.idf as number;
        } catch (error) {
            console.error("Error getting idf entry:", error);
        }

        return 0;
    }

    async updateGlobalStatsEntry(global_entry: GlobalStatisticsEntry): Promise<void> 
    {   
        const params = {
            TableName: this.tableName,
            Item: global_entry
        };

        try {
            await this.client.send(new PutCommand(params));
        } catch (error) {
            console.error("Error updating global stats entry:", error);
        }
    }

    async putDynamoDBEntryBatch(requests: (InverseDocumentFrequencyEntry | TermFrequencyEntry)[]): Promise<void> 
    {
        for (let i = 0; i < requests.length; i += DYNAMO_DB_MAX_BATCH_SIZE) 
        {
            console.log(`Processing batch from index ${i} of ${requests.length}`);
            const batch = requests.slice(i, i + DYNAMO_DB_MAX_BATCH_SIZE);
            const put_requests = batch.map(entry => {
                return {
                    PutRequest: {
                        Item: entry
                    }
                };
            });

            const params = {
                TableName : this.tableName,
                RequestItems: {
                    [this.tableName]: put_requests
                }
            };

            try {
                //TODO: handle unprocessed items + retry
                const result = await this.client.send(new BatchWriteCommand(params));
            } catch (error) {
                console.error("Error updating idf entry:", error);
            }
        }
    }

    async deleteDynamoDBEntryBatch(requests: (InverseDocumentFrequencyEntry | TermFrequencyEntry)[]): Promise<void> 
    {
        for (let i = 0; i < requests.length; i += DYNAMO_DB_MAX_BATCH_SIZE) 
        {
            const batch = requests.slice(i, i + DYNAMO_DB_MAX_BATCH_SIZE);
            const deleteRequests = batch.map(entry => ({
            DeleteRequest: {
                Key: {
                    pk: entry.pk,
                    id: entry.id
                }
            }
            }));
            // DynamoDB allows a maximum of 25 items per batch write
            if (deleteRequests.length > 25) {
            throw new Error(`Batch size exceeds maximum of 25 items. Current size: ${deleteRequests.length}`);
            }
            // Create the params for the BatchWriteCommand
            const params = {
            RequestItems: {
                [this.tableName]: deleteRequests
            }
            };

            try {
            await this.client.send(new BatchWriteCommand(params));
            }
            catch (error) {
            console.error("Error deleting entries from DynamoDB", { error });
            throw error;
            }
        }
    }
}


