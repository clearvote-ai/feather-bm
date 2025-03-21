import { BatchWriteCommand, DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBIDFEntry, DynamoDBIndexEntry } from "../DynamoDBIndex";
import { InvertedIndexGlobalStatistics } from "../../../BM25/InvertedIndex";

export async function putDynamoDBIndexEntryBatch(client : DynamoDBDocumentClient, table_name: string, batch: DynamoDBIndexEntry[]): Promise<void> {
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
        console.error("Error updating index entry:", error);
    }
}

export async function putDynamoDBIDFEntryBatch(client : DynamoDBDocumentClient, table_name: string, batch: DynamoDBIDFEntry[]): Promise<void> {
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

export async function updateGlobalStatsEntry(client: DynamoDBDocumentClient, table_name: string, index_name: string, stats: InvertedIndexGlobalStatistics): Promise<void> {
    const placeholder_0_id = new Uint8Array(16);
    const global_entry = {
        pk: `${index_name}#global_stats`,
        id: placeholder_0_id,
        totalDocumentLength: stats.totalDocumentLength,
        documentCount: stats.documentCount,
    };

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