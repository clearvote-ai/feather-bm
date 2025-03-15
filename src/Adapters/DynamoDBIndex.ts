import { BatchWriteCommand, DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { buildInvertedEndexEntries, IndexedDocument, InverseDocumentValue, InvertedIndex, InvertedIndexEntry, InvertedIndexGlobalStatistics } from "../BM25/InvertedIndex";
import { FeatherBMIndex } from "./Adapter";


type DynamoDBIndexToken = string;

//TODO: combine tokens into one flat entry
export interface DynamoDBIndexEntry {
    index_name: string,
    sortkey: `${DynamoDBIndexToken}`,
    documents: InverseDocumentValue[],
    idf: number
}

export interface DynamoDBIndexGlobalEntryFields {
    index_name: string,
    sortkey: "global",
}

export type DynamoDBIndexGlobalEntry = DynamoDBIndexGlobalEntryFields & InvertedIndexGlobalStatistics;

export class DynamoDBIndex extends FeatherBMIndex
{
    
    client: DynamoDBDocumentClient;
    table_name: string;
    index_name: string;
    //the count in tokens of all documents added to the index
    totalDocumentLength: number;
    documentCount: number;

    private constructor(client: DynamoDBDocumentClient, table_name: string, index_name: string, averageDocumentLength: number, documentCount: number)
    {
        super();
        this.client = client;
        this.table_name = table_name;
        this.index_name = index_name;
        this.totalDocumentLength = averageDocumentLength;
        this.documentCount = documentCount;
    }

    static async from(client: DynamoDBDocumentClient, table_name: string, index_name: string): Promise<DynamoDBIndex>
    {
        const index = new DynamoDBIndex(client, table_name, index_name, 0, 0);
        const global_entry = await index.getGlobalEntry();
        index.totalDocumentLength = global_entry.totalDocumentLength;
        index.documentCount = global_entry.documentCount;

        return index;
    }

    async getEntry(token: string): Promise<InvertedIndexEntry | undefined> {
        const entries = await this.getIndexEntry(token);
        if(entries.length === 0) return undefined;

        //if multiple entries are returned, combine them into a single entry
        const combined_entry = entries.reduce((acc, entry) => {
            if(acc.documents === undefined) acc.documents = [];
            for(const doc_id in entry.documents)
            {
                //there should be no duplicate doc_ids
                acc.documents.push(entry.documents[doc_id]);
            }

            return acc;
        });

        //IDF is only on the first entry
        combined_entry.idf = entries[0].idf;

        return combined_entry;
    }

    async getAverageDocumentLength(): Promise<number> {
        return this.totalDocumentLength / this.documentCount;
    }

    async insert(document: IndexedDocument): Promise<void> {
        this.insert_batch([document]);
    }

    async insert_batch(documents: IndexedDocument[]): Promise<void> {

        const { index , global_stats } = buildInvertedEndexEntries(documents);
        const entries = Object.keys(index);

        //TODO: split DB entries further into timestamped bins and global IDF entries
        const db_entries : DynamoDBIndexEntry[] = entries.map(token => {
            return {
                index_name: this.index_name,
                sortkey: token,
                documents: index[token].documents,
                idf: index[token].idf
            };
        });

        //split into batches of 25
        const MAX_BATCH_SIZE = 25;
        for(let i = 0; i < entries.length; i += MAX_BATCH_SIZE)
        {
            console.log(`Inserting batch ${i} to ${i + MAX_BATCH_SIZE}`);
            const batch = db_entries.slice(i, i + MAX_BATCH_SIZE);
            await this.putDynamoDBIndexEntryBatch(batch);
        }

        //update the global entry
        await this.updateGlobalEntry(global_stats);
    }
    
    delete(sortkey: string): Promise<void> {
        throw new Error("Method not implemented.");
    }

    private async putDynamoDBIndexEntryBatch(batch: DynamoDBIndexEntry[]): Promise<void> {
        const put_requests = batch.map(entry => {
            return {
                PutRequest: {
                    Item: entry
                }
            };
        });

        const params = {
            TableName : this.table_name,
            RequestItems: {
                [this.table_name]: put_requests
            }
        };

        try {
            //TODO: handle unprocessed items + retry
            const result = await this.client.send(new BatchWriteCommand(params));
        } catch (error) {
            console.error("Error:", error);
        }
    }


    private async getIndexEntry(token: string): Promise<DynamoDBIndexEntry[]> {
        const params = {
            TableName: this.table_name,
            KeyConditionExpression: "index_name = :index_name AND begins_with(sortkey, :sortkey)",
            ExpressionAttributeValues: {
                ":index_name": this.index_name,
                ":sortkey": token
            },
        };
        
        try {
            const data = await this.client.send(new QueryCommand(params));
            if(data.Items === undefined) return [];
            return data.Items as DynamoDBIndexEntry[];
        } catch (error) {
            console.error("Error:", error);
        }

        return [];
    }

    private async getGlobalEntry(): Promise<DynamoDBIndexGlobalEntry> {
        const params = {
            TableName: this.table_name,
            Key: {
                index_name: this.index_name,
                sortkey: "global"
            }
        };
        
        try {
            const data = await this.client.send(new GetCommand(params));
            if(data.Item === undefined) return { index_name: this.index_name, sortkey: "global", totalDocumentLength: 0 , documentCount: 0};
            return data.Item as DynamoDBIndexGlobalEntry;
        } catch (error) {
            console.error("Error:", error);
        }

        return { index_name: this.index_name, sortkey: "global", totalDocumentLength: 0 , documentCount: 0};
    }

    private async updateGlobalEntry(stats: InvertedIndexGlobalStatistics): Promise<void> {
        const global_entry = {
            index_name: this.index_name,
            sortkey: "global",
            totalDocumentLength: stats.totalDocumentLength,
            documentCount: stats.documentCount,
        };

        const params = {
            TableName: this.table_name,
            Item: global_entry
        };

        try {
            await this.client.send(new PutCommand(params));
        } catch (error) {
            console.error("Error:", error);
        }
    }
    
}