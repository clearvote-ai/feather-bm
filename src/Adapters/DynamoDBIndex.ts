import { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { buildInvertedIndexLegacy, IndexedDocument, InvertedIndex, InvertedIndexEntry } from "../BM25/InvertedIndex";
import { FeatherBMIndex } from "./Adapter";


type DynamoDBIndexDocumentUUID = string;
type DynamoDBIndexToken = string;

interface DynamoDBIndexTokenEntry {
    index_name: string,
    sortkey: `${DynamoDBIndexToken}#${DynamoDBIndexDocumentUUID}`,
    termFrequency: number,
    documentLength: number
}

interface DynamoDBIndexInverseDocumentFrequencyEntry {
    index_name: string,
    sortkey: `${DynamoDBIndexToken}#IDF`,
    inverseDocumentFrequency: number
}

type DynamoDBIndexEntry = DynamoDBIndexTokenEntry | DynamoDBIndexInverseDocumentFrequencyEntry;


export class DynamoDBIndex extends FeatherBMIndex
{
    
    client: DynamoDBDocumentClient;
    table_name: string;
    index_name: string;
    averageDocumentLength: number;

    private constructor(client: DynamoDBDocumentClient, table_name: string, index_name: string, averageDocumentLength: number)
    {
        super();
        this.client = client;
        this.table_name = table_name;
        this.index_name = index_name;
        this.averageDocumentLength = averageDocumentLength;
    }

    static async from(client: DynamoDBDocumentClient, table_name: string, index_name: string): Promise<DynamoDBIndex>
    {
        const index = new DynamoDBIndex(client, table_name, index_name, 0);
        index.averageDocumentLength = await index.lookupAverageDocumentLength();

        return index;
    }

    async getEntry(token: string): Promise<InvertedIndexEntry | undefined> {
        const db_entries = await this.getDynamoDBIndexEntries(token);
        if(db_entries.length === 0) return undefined;

        const entry : InvertedIndexEntry = {
            documents: {},
            inverseDocumentFrequency: 0
        };

        for(const db_entry of db_entries)
        {
            if(db_entry.sortkey.endsWith("#IDF")) {
                entry.inverseDocumentFrequency = (db_entry as DynamoDBIndexInverseDocumentFrequencyEntry).inverseDocumentFrequency;
            } else {
                const [token, doc_id] = db_entry.sortkey.split("#");
                if(!entry.documents.hasOwnProperty(doc_id)) entry.documents[doc_id] = { termFrequency: 0, documentLength: 0 };
                entry.documents[doc_id] = {
                    termFrequency: (db_entry as DynamoDBIndexTokenEntry).termFrequency,
                    documentLength: (db_entry as DynamoDBIndexTokenEntry).documentLength
                }
            }
        }

        return entry;
    }

    private async lookupAverageDocumentLength(): Promise<number>
    {
        const params = {
            TableName: this.table_name,
            KeyConditionExpression: "index_name = :index_name AND begins_with(sortkey, :sortkey)",
            ExpressionAttributeValues: {
                ":index_name": this.index_name,
                ":sortkey": "averageDocumentLength"
            },
          };
        
        try {
            const data = await this.client.send(new QueryCommand(params));
            if(data.Items === undefined) return 0;
            return data.Items[0].averageDocumentLength as number;
        } catch (error) {
            console.error("Error:", error);
        }

        return 0;
    }

    async getAverageDocumentLength(): Promise<number> {
        return this.averageDocumentLength;
    }

    async insert_document(sortkey: string, full_text: string): Promise<void> {
        //TODO: 1. take the full_text and split it into InvertedIndexEntries
        //TODO: 2. Split the inverted index entries into DynamoDBIndexEntries
        //TODO: 3. Batch and combine the entries into a single batch write
        //TODO: 4. Write the batch to the DynamoDB table

        const values = [{ sortkey: sortkey, full_text: full_text } as IndexedDocument];
        const index_entries = buildInvertedIndexLegacy(values);
        const entry_count = Object.keys(index_entries.invertedIndex).length;
        const db_entries : DynamoDBIndexEntry[] = [];

        for(const [token, entry] of Object.entries(index_entries.invertedIndex))
        {
            db_entries.push({
                sortkey: `${token}#IDF`,
                inverseDocumentFrequency: entry.inverseDocumentFrequency
            } as DynamoDBIndexInverseDocumentFrequencyEntry);

            for(const [doc_id, doc_entry] of Object.entries(entry.documents))
            {
                db_entries.push({
                    sortkey: `${token}#${doc_id}`,
                    termFrequency: doc_entry.termFrequency,
                    documentLength: doc_entry.documentLength
                } as DynamoDBIndexTokenEntry);
            }
        }

        const writes = db_entries.map(entry => {
            return {
                PutRequest: {
                    Item: entry
                }
            }
        });
    }
    
    delete_document(sortkey: string): Promise<void> {
        throw new Error("Method not implemented.");
    }

    async getDynamoDBIndexEntries(token: string): Promise<DynamoDBIndexEntry[]> {
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
    
}