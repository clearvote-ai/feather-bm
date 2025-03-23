import { DynamoDBDocumentClient, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBIndexEntry, DynamoDBIndexGlobalEntry } from "../DynamoDBIndex";

export async function getIndexEntry(client: DynamoDBDocumentClient, table_name: string, indexName: string, token: string): Promise<DynamoDBIndexEntry[]> {
    const params = {
        TableName: table_name,
        KeyConditionExpression: "pk = :pk",
        ExpressionAttributeValues: {
            ":pk": `${indexName}#${token}`
        },
    };
    
    try {
        const data = await client.send(new QueryCommand(params));
        if(data.Items === undefined) return [];
        return data.Items as DynamoDBIndexEntry[];
    } catch (error) {
        console.error("Error getting index entry:", error);
    }

    return [];
}

export async function getDynamoDBInverseDocumentFrequencyEntry(client: DynamoDBDocumentClient, table_name: string, indexName: string, token: string): Promise<number> {
    
    const placeholder_0_id = new Uint8Array(16);

    const params = {
        TableName: table_name,
        Key: {
            pk: `${indexName}#${token}`,
            id: placeholder_0_id
        }
    };

    try {
        const data = await client.send(new GetCommand(params));
        if(data.Item === undefined) return 0;
        return data.Item.idf as number;
    } catch (error) {
        console.error("Error getting idf entry:", error);
    }

    return 0;
}

export async function getGlobalStatsEntry(client: DynamoDBDocumentClient, table_name: string, indexName: string): Promise<DynamoDBIndexGlobalEntry> {
    const placeholder_0_id = new Uint8Array(16);
    const params = {
        TableName: table_name,
        Key: {
            pk: `${indexName}#global_stats`,
            id: placeholder_0_id
        }
    };
    
    try {
        const data = await client.send(new GetCommand(params));
        if(data.Item === undefined) return { pk: `${indexName}#global_stats`, totalDocumentLength: 0 , documentCount: 0};
        return data.Item as DynamoDBIndexGlobalEntry;
    } catch (error) {
        console.error("Error reading global stats entry:", error);
    }

    return { pk: `${indexName}#global_stats`, totalDocumentLength: 0 , documentCount: 0};
}