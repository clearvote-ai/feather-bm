import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { GlobalStatisticsEntry, InverseDocumentFrequencyEntry, TermFrequencyEntry, UUID_000 } from "../../../FeatherTypes";
import { FeatherBMIndex } from "../../FeatherBMIndex";
import { DynamoDBRead } from "./Helpers/Read";
import { DynamoDBCreate } from "./Helpers/Create";
import { DynamoDBDelete } from "./Helpers/Delete";

export const DYNAMO_DB_MAX_BATCH_SIZE = 25;
export class DynamoDBIndex extends FeatherBMIndex
{
    client: DynamoDBDocumentClient;
    tableName: string;
    
    constructor(client: DynamoDBDocumentClient, tableName: string, indexName: string, averageDocumentLength: number, documentCount: number)
    {
        super(indexName, averageDocumentLength, documentCount);
        this.tableName = tableName;
        this.client = client;
    }

    async getEntries(token: string): Promise<{ idf_entry: InverseDocumentFrequencyEntry; tf_entries: TermFrequencyEntry[]; }> {
        try {
            const idf_entry = await DynamoDBRead.getInverseDocumentFrequencyEntry(this.client, this.tableName, this.indexName, token);
            const tf_entries = await DynamoDBRead.getTermFrequencyEntries(this.client, this.tableName, this.indexName, token);
            return { idf_entry: { pk: `${this.indexName}#${token}`, id: UUID_000, idf: idf_entry }, tf_entries };
        } catch (error) {
            console.error("Error getting entries:", error);
            throw new Error("Failed to retrieve entries from DynamoDB.");
        }
    }

    async insert_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[]): Promise<void> {
        await DynamoDBCreate.putDynamoDBEntryBatch(this.client, this.tableName, idf_entries);
        await DynamoDBCreate.putDynamoDBEntryBatch(this.client, this.tableName, tf_entries);
    }

    async delete_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[]): Promise<void> {
        await DynamoDBDelete.deleteDynamoDBEntryBatch(this.client, this.tableName, idf_entries);
        await DynamoDBDelete.deleteDynamoDBEntryBatch(this.client, this.tableName, tf_entries);
    }

    async update_global_entry_internal(global_stats: GlobalStatisticsEntry): Promise<void> {
        await DynamoDBCreate.updateGlobalStatsEntry(this.client, this.tableName, global_stats);
    }

    static async from(client: DynamoDBDocumentClient, tableName: string, indexName: string): Promise<DynamoDBIndex>
    {
        const global_entry = await DynamoDBRead.getGlobalStatsEntry(client, tableName, indexName);
        const index = new DynamoDBIndex(client, tableName, indexName, global_entry.totalDocumentLength, global_entry.documentCount);
        return index;
    }
}


