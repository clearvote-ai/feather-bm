import { DynamoDBDocumentClient, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { GlobalStatisticsEntry, TermFrequencyEntry, UUID_000 } from "../../../FeatherBMIndex.d";

export namespace DynamoDBRead {
    export async function getInverseDocumentFrequencyEntry(client: DynamoDBDocumentClient, table_name: string, indexName: string, token: string): Promise<number> {
            
        const params = {
            TableName: table_name,
            Key: {
                pk: `${indexName}#${token}`,
                id: UUID_000 // placeholder for idf entry
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

    export async function getTermFrequencyEntries(client: DynamoDBDocumentClient, table_name: string, indexName: string, token: string): Promise<TermFrequencyEntry[]> {
        const params = {
            TableName: table_name,
            KeyConditionExpression: "pk = :pk",
            ExpressionAttributeValues: {
                ":pk": `${indexName}#${token}`,
            },
        };

        try {
            const data = await client.send(new QueryCommand(params));
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

    export async function getGlobalStatsEntry(client: DynamoDBDocumentClient, table_name: string, indexName: string): Promise<GlobalStatisticsEntry> {
        const params = {
            TableName: table_name,
            Key: {
                pk: `${indexName}#global_stats`,
                id: UUID_000
            }
        };
        
        try {
            const data = await client.send(new GetCommand(params));
            if(data.Item === undefined) throw new Error("Global stats entry not found");
            return data.Item as GlobalStatisticsEntry;
        } catch (error) {
            console.error("Error reading global stats entry:", error);
        }

        return { pk: `${indexName}#global_stats`, id: UUID_000, totalDocumentLength: 0, documentCount: 0}; // return a default value if not found
    }
}