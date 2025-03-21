import { BatchWriteCommand, DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { InverseDocumentValue, InvertedIndex, InvertedIndexEntry, InvertedIndexGlobalStatistics } from "../../BM25/InvertedIndex";
import { FeatherBMIndex } from "../FeatherBMIndex";
import { putDynamoDBIDFEntryBatch, putDynamoDBIndexEntryBatch, updateGlobalStatsEntry } from "./Helpers/Create";
import { getDynamoDBInverseDocumentFrequencyEntry, getGlobalStatsEntry, getIndexEntry } from "./Helpers/Read";
import { uuidv7 } from "uuidv7";
import { parse } from 'uuid';
import { BinaryAttributeValue } from "aws-sdk/clients/dynamodb";
import { stringify } from 'uuid';


type DynamoDBIndexToken = string;
type DynamoDBIndexID = string;

//partition key for the search table is the index name + token
export type DynamoDBIndexPartitionKey = `${DynamoDBIndexID}#${DynamoDBIndexToken}`;

//TODO: combine tokens into one flat entry
export interface DynamoDBIndexEntry {
    pk: DynamoDBIndexPartitionKey, //partition key
    id: BinaryAttributeValue, //sort key UUIDv7
    tf: BinaryAttributeValue, //12 byte GSI sort key first 2 bytes are term frequency, next 4 bytes are document len, next 6 bytes are the timestamp
}

export interface DynamoDBIDFEntry {
    pk: DynamoDBIndexPartitionKey, //partition key
    id: BinaryAttributeValue, //sort key placeholder for idf
    idf: number //inverse document frequency
}

export type DynamoDBEntry = DynamoDBIndexEntry | DynamoDBIDFEntry;

export interface DynamoDBIndexGlobalEntryFields {
    //TODO: make the token "global_stats" a protected token
    pk: `${DynamoDBIndexID}#global_stats`, //partition key
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
        const global_entry = await getGlobalStatsEntry(client, table_name, index_name);
        index.totalDocumentLength = global_entry.totalDocumentLength;
        index.documentCount = global_entry.documentCount;

        return index;
    }

    async getEntry(token: string): Promise<InvertedIndexEntry | undefined> 
    {
        const entries = await getIndexEntry(this.client, this.table_name, this.index_name, token);
        if(entries.length === 0) return undefined;

        //if the first entry is the idf entry, assign idf otherwise get the entry directly
        const possible_idf = entries[0] as unknown as DynamoDBIDFEntry;

        const idf = possible_idf.idf !== undefined ? 
            possible_idf.idf : 
            await getDynamoDBInverseDocumentFrequencyEntry(this.client, this.table_name, this.index_name, token);
       
        if(possible_idf.idf !== undefined) entries.shift(); //remove the idf entry from the list

        //combine the entries into an InvertedIndexEntry
        const inverse_document_values = entries.map(entry => {
            const id_byte_array = entry.id as Uint8Array;

            //convert the binary id to a string
            const id = stringify(id_byte_array);

            //strip off the first 2 bytes of the id to get the term frequency
            const tf_bytes = entry.tf as Uint8Array;
            const tf = (tf_bytes[0] | (tf_bytes[1] << 8)) & 0xFFFF; //mask to get the last 16 bits
            //strip off the next 4 bytes of the id to get the document length
            const len = (tf_bytes[2] | (tf_bytes[3] << 8) | (tf_bytes[4] << 16) | (tf_bytes[5] << 24)) & 0xFFFFFFFF; //mask to get the last 32 bits
            
            return { id, tf, len } as InverseDocumentValue;
        });

        

        return { documents: inverse_document_values, idf };
        
    }

    async getAverageDocumentLength(): Promise<number> {
        return this.totalDocumentLength / this.documentCount;
    }

    async insert_batch_internal(insert_entries: InvertedIndex, global_stats: InvertedIndexGlobalStatistics): Promise<void> {

        const entries = Object.keys(insert_entries);

        const index_entries : DynamoDBIndexEntry[] = entries.map(token => {
            const entry = insert_entries[token];
            const documents = entry.documents;

            return documents.map(doc => {
                const uuid = uuidv7();
                const uuid_bytes = parse(uuid);

                const tf_binary = new Uint8Array(12);
                //first 2 bytes are the term frequency
                const tf_value = doc.tf & 0xFFFF; //mask to get the last 16 bits
                tf_binary.set([tf_value & 0xFF, (tf_value >> 8) & 0xFF], 0);
                //next 4 bytes are the document length
                const doc_len = doc.len & 0xFFFFFFFF; //mask to get the last 32 bits
                tf_binary.set([
                    doc_len & 0xFF,
                    (doc_len >> 8) & 0xFF,
                    (doc_len >> 16) & 0xFF,
                    (doc_len >> 24) & 0xFF
                ], 2);
                //next 6 bytes are the timestamp
                //copy first 6 bytes of the uuid to the tf binary
                const timestamp = uuid_bytes.slice(0, 6);
                tf_binary.set(timestamp, 6);
                return {
                    pk: `${this.index_name}#${token}`,
                    id: uuid_bytes,
                    tf: tf_binary
                } satisfies DynamoDBIndexEntry;
            });
        }).flat();

        console.log(`Inserting ${index_entries.length} entries into DynamoDB`);

        //split into batches of 25
        const MAX_BATCH_SIZE = 25;
        for( let i = 0; i < index_entries.length; i += MAX_BATCH_SIZE )
        {
            if(i % 2000 === 0) console.log(`Inserting batch ${i} of ${index_entries.length}`);
            const batch = index_entries.slice(i, i + MAX_BATCH_SIZE);
            await putDynamoDBIndexEntryBatch(this.client, this.table_name, batch);
        }

        const idf_entries : DynamoDBIDFEntry[] = entries.map(token => {
            const idf = insert_entries[token].idf;
            const placeholder_0_id = new Uint8Array(16);
            return {
                pk: `${this.index_name}#${token}`,
                id: placeholder_0_id,
                idf
            } satisfies DynamoDBIDFEntry;
        });

        //split into batches of 25
        for( let i = 0; i < idf_entries.length; i += MAX_BATCH_SIZE )
        {
            if(i % 2000 === 0) console.log(`Inserting batch ${i} of ${idf_entries.length}`);
            const batch = idf_entries.slice(i, i + MAX_BATCH_SIZE);
            await putDynamoDBIDFEntryBatch(this.client, this.table_name, batch);
        }

        //update the global entry
        await updateGlobalStatsEntry(this.client, this.table_name, this.index_name, global_stats);
    }

    async delete_batch_internal(delete_entries: InvertedIndex, global_stats: InvertedIndexGlobalStatistics): Promise<void> {
        
        //we recieve an index built from the documents to delete
        const entries = Object.keys(delete_entries);

        //each entry is a token + timestamp thats been effected by the delete operation
        //we only want to remove documents from the index entry associated with the token and timestamp NOT the entire token entry itself
        

        

        
    }
    
}


