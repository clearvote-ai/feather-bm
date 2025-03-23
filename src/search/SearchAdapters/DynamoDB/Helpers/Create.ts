import { BatchWriteCommand, DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { GlobalStatisticsEntry, InverseDocumentFrequencyEntry, TermFrequencyEntry } from "../../../../FeatherTypes";
import { DYNAMO_DB_MAX_BATCH_SIZE } from "../DynamoDBIndex";

export namespace DynamoDBCreate {
    export async function putDynamoDBEntryBatch(client : DynamoDBDocumentClient, table_name: string, requests: (InverseDocumentFrequencyEntry | TermFrequencyEntry)[]): Promise<void> {

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
                TableName : table_name,
                RequestItems: {
                    [table_name]: put_requests
                }
            };

            try {
                //TODO: handle unprocessed items + retry
                const result = await client.send(new BatchWriteCommand(params));
            } catch (error) {
                console.error("Error updating idf entry:", error);
            }
        }
    }

    export async function updateGlobalStatsEntry(client: DynamoDBDocumentClient, table_name: string, global_entry: GlobalStatisticsEntry): Promise<void> 
    {   
        const params = {
            TableName: table_name,
            Item: global_entry
        };

        try {
            await client.send(new PutCommand(params));
        } catch (error) {
            console.error("Error updating global stats entry:", error);
        }
    }
}