import { DynamoDBDocumentClient, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { GlobalStatisticsEntry, InverseDocumentFrequencyEntry, TermFrequencyEntry, UUID_000 } from "../../../FeatherTypes";
import { FeatherBMIndex } from "../FeatherBMIndex";
import { DynamoDBHelpers } from "./Helpers/Read";
export class DynamoDBIndex extends FeatherBMIndex
{
    
    client: DynamoDBDocumentClient;
    tableName: string;
    MAX_BATCH_SIZE = 25;

    constructor(client: DynamoDBDocumentClient, tableName: string, indexName: string, averageDocumentLength: number, documentCount: number)
    {
        super(indexName, averageDocumentLength, documentCount);
        this.tableName = tableName;
        this.client = client;
    }

    

    getEntries(token: string): Promise<{ idf_entry: InverseDocumentFrequencyEntry; tf_entries: TermFrequencyEntry[]; }> {
        return new Promise(async (resolve, reject) => {
            try {
                const idf_entry = await DynamoDBHelpers.getInverseDocumentFrequencyEntry(this.client, this.tableName, this.indexName, token);
                const tf_entries = await DynamoDBHelpers.getTermFrequencyEntries(this.client, this.tableName, this.indexName, token);
                resolve({ idf_entry: { pk: `${this.indexName}#${token}`, id: UUID_000, idf: idf_entry }, tf_entries });
            } catch (error) {
                console.error("Error getting entries:", error);
                reject(error);
            }
        });
    }

    insert_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[], global_stats: GlobalStatisticsEntry): Promise<void> {
        throw new Error("Method not implemented.");
    }
    delete_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[], global_stats: GlobalStatisticsEntry): Promise<void> {
        throw new Error("Method not implemented.");
    }

    static async from(client: DynamoDBDocumentClient, tableName: string, indexName: string): Promise<DynamoDBIndex>
    {
        const global_entry = await DynamoDBHelpers.getGlobalStatsEntry(client, tableName, indexName);
        const index = new DynamoDBIndex(client, tableName, indexName, global_entry.totalDocumentLength, global_entry.documentCount);
        return index;
    }
    
}


