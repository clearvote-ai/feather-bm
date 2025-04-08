import { BM25Score, GlobalStatisticsEntry, IndexEntry, InverseDocumentFrequencyEntry, TermFrequencyEntry } from "./FeatherBMIndex.d";
import { expandQueryToTokens } from "./NLPUtils";
import PromisePool from "@supercharge/promise-pool";
import { parse, stringify } from "uuid";
import { IngestionDocument } from "../documents/FeatherDocumentStore.d";

//Default UUID for IDF and Global stats entries
export const UUID_000 = new Uint8Array(16);
export type UUID_000 = typeof UUID_000;

export abstract class FeatherBMIndex
{
    //change this depending on your DB API rate limits and core count
    MAX_CONCURRENT_QUERIES = 4;

    //BM25 parameters
    //Want to understand the significance of the params below?: https://www.youtube.com/watch?v=ruBm9WywevM
    K1 = 1.2;
    B = 0.75;
    DEFAULT_AVERAGE_DOCUMENT_LENGTH = 400; //default average document length

    indexName: string;
    //the count in tokens of all documents added to the index
    totalDocumentLength: number;
    documentCount: number;

    //Your adapter must implement these methods to interact with your data store
    abstract getEntries(token: string, max_results?: number) : Promise<{ idf_entry: InverseDocumentFrequencyEntry, tf_entries: TermFrequencyEntry[] }>;
    abstract getEntriesGlobal(token: string, max_results?: number) : Promise<{ idf_entry: InverseDocumentFrequencyEntry, tf_entries: TermFrequencyEntry[] }>;
    
    abstract update_global_entry_internal(global_stats: GlobalStatisticsEntry) : Promise<void>;
    abstract insert_internal(tf_entries: TermFrequencyEntry[], idf_entries:InverseDocumentFrequencyEntry[]) : Promise<void>;
    abstract delete_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[]) : Promise<void>;
    

    constructor(indexName: string, totalDocumentLength: number, documentCount: number, K1: number = 1.2, B: number = 0.75)
    {
        this.indexName = indexName;
        this.totalDocumentLength = totalDocumentLength;
        this.documentCount = documentCount;
        this.K1 = K1;
        this.B = B;
    }

    getAverageDocumentLength(): number {
        const averageDocumentLength = this.totalDocumentLength / this.documentCount;
        if (isNaN(averageDocumentLength)) {
            return this.DEFAULT_AVERAGE_DOCUMENT_LENGTH;
        }
        return Math.max(this.totalDocumentLength / this.documentCount, this.DEFAULT_AVERAGE_DOCUMENT_LENGTH); //default to 400 if no documents
    }

    async updateGlobalEntry(global_stats: GlobalStatisticsEntry, insert: boolean) : Promise<void> {
        
        const new_document_count = insert ? this.documentCount + global_stats.documentCount : this.documentCount - global_stats.documentCount;
        const new_total_document_length = insert ? this.totalDocumentLength + global_stats.totalDocumentLength : this.totalDocumentLength - global_stats.totalDocumentLength;
        if(new_document_count <= 0) throw new Error("Document count cannot be less than 0");
        if(new_total_document_length <= 0) throw new Error("Total document length cannot be less than 0");
        const new_global_stats_entry : GlobalStatisticsEntry = {
            pk: `${this.indexName}#global_stats`, //partition key
            id: UUID_000, //sort key placeholder for global stats
            totalDocumentLength: new_total_document_length,
            documentCount: new_document_count
        };

        //update the internal state of the index
        this.documentCount = new_document_count;
        this.totalDocumentLength = new_total_document_length;

        //Adapter is responsible for updating the global stats entry in the data store
        await this.update_global_entry_internal(new_global_stats_entry);
    }
        

    //NOTE: you should ensure the documents are NOT already in the index before calling insert
    //calling insert here will overwrite any existing documents with the same id but it might result in undefined index behavior
    //if you inserted 2 different versions of the same document with the same UUID then the index will keep both versions which you may not want
    async insert(documents: IngestionDocument[] | IngestionDocument) : Promise<void>{
        if(!Array.isArray(documents)) documents = [documents]; //ensure we have an array of documents
        if(documents.length === 0) return; //nothing to insert
        const { global_stats_entry, idf_entries, tf_entries } = this.computeInvertedEndexEntries(documents);

        //Adapter is responsible for inserting the entries into the data store
        await this.insert_internal(tf_entries, idf_entries);

        //update the global stats entry in the data store
        await this.updateGlobalEntry(global_stats_entry, true); //true = insert
    }

    //NOTE: you should ensure the documents are already in the index before calling delete
    async delete(documents: IngestionDocument[] | IngestionDocument ) : Promise<void> {
        if(!Array.isArray(documents)) documents = [documents]; //ensure we have an array of documents
        if(documents.length === 0) return; //nothing to delete
        const { global_stats_entry, idf_entries, tf_entries } = this.computeInvertedEndexEntries(documents);

        //Adapter is responsible for deleting the entries from the data store
        await this.delete_internal(tf_entries, idf_entries);

        //update the global stats entry in the data store
        await this.updateGlobalEntry(global_stats_entry, false); //false = delete
    }

    //compute the BM25 scores for every relevant document in our inverted index for a given query CONCURRENTLY
    async query(query: string, global: boolean = false, max_results?: number) : Promise<BM25Score[]> { 
        const query_tokens = expandQueryToTokens(query);

        //split the query into tokens and get the BM25 scores for each token concurrently
        // this computes BM25 for each document in the tokens lookup table
        const { results } = await PromisePool
        .withConcurrency(this.MAX_CONCURRENT_QUERIES)
        .for(query_tokens)
        .handleError(async (error, token, pool) => {
            console.error("Error processing token index lookup", { error });
        })
        .process(async (token, index, pool) => await this.queryToken(token, global, max_results));
        
        if(!results) throw new Error("No scores found for query");

        //flatten out the results and combine the scores for documents that appear in multiple tokens
        const flattenedResults = results.flat();
        let scores : BM25Score[] = flattenedResults.reduce((acc, score) => {
            const existing_score = acc.find(s => s.id === score.id);
            if(existing_score) existing_score.score += score.score;
            else acc.push(score);
            return acc;
        }, [] as BM25Score[]);

        //return the sorted list of documents by score
        return scores.sort((a, b) => b.score - a.score).slice(0, max_results);
    }

    //query the inverted index for a single token and compute the BM25 scores for all documents that contain that token
    //this is called by the query function above for each token in the query
    //it returns an array of BM25 scores for each document that contains the token
    private async queryToken(token: string, global: boolean, max_results?: number) : Promise<BM25Score[]> {
        //const averageDocumentLength = this.getAverageDocumentLength();

        const { idf_entry, tf_entries } = global ? await this.getEntriesGlobal(token, max_results) : await this.getEntries(token, max_results) 
        // Token not found in inverted index
        if(tf_entries === undefined || tf_entries === null || tf_entries.length === 0) return [];

        //split out the idf entry from the rest of the entries
        //IDF Entry is the first entry in the list
        if(idf_entry === undefined || idf_entry.idf === undefined || idf_entry.id !== UUID_000) throw new Error(`No IDF entry found for token: ${token}`);
        
        //compute the BM25 score for each document in the inverted index for this token
        const scores : BM25Score[] = tf_entries.map(entry => {
            const tf_indexed = entry.tf;
            const uuid = stringify(entry.id);
            const score = tf_indexed * idf_entry.idf;
            return { id: uuid, score: score };
        });

        return scores;
    }

    //compute the BM25 score for a single TOKEN_X_DOCUMENT pair
    bm25(inverseDocumentFrequency: number, termFrequency: number, documentLength: number, averageDocumentLength: number) : number
    {
        return (
            (inverseDocumentFrequency * (termFrequency * (this.K1 + 1))) /
            (termFrequency + this.K1 * (1 - this.B + (this.B * documentLength) / averageDocumentLength))
        );
    }

    bm25Static(termFrequency: number, documentLength: number, averageDocumentLength: number) : number
    {
        return (
            (termFrequency * (this.K1 + 1)) / (termFrequency + this.K1 * (1 - this.B + (this.B * documentLength) / averageDocumentLength))
        );
    }

    //the function that actually builds the inverted index entries for a set of documents
    computeInvertedEndexEntries(documents: IngestionDocument[])
    {
        const term_frequency_entries : TermFrequencyEntry[] = [];
        const idf_entries : InverseDocumentFrequencyEntry[] = [];
        if(documents.length === 0) throw new Error("No documents to index");
        if(documents.some(doc => !doc.uuidv7)) throw new Error("All documents must have a uuidv7");

        const invertedIndexIDFEntries : { [token: string] : Set<string> } = {};
        var totalDocumentLength = 0;
        const averageDocumentLength = this.getAverageDocumentLength();

        //TODO: make this a concurrent operation
        for(const doc of documents)
        {
            const words = expandQueryToTokens(doc.text);
            const token_count = words.length;
            const doc_tf_entries : { [word: string] : number } = {};

            for(const token of words)
            {
                //increment the InverseDocumentFrequency (IDF) for this token aka the number of documents that contain this token
                if(!invertedIndexIDFEntries.hasOwnProperty(token)) invertedIndexIDFEntries[token] = new Set<string>();
                invertedIndexIDFEntries[token].add(doc.uuidv7);
                if(!doc_tf_entries.hasOwnProperty(token)) doc_tf_entries[token] = 0;
                doc_tf_entries[token]++;
            }

            totalDocumentLength += token_count;
            for(const token in doc_tf_entries)
            {
                //we assume that the id is already a UUIDv7, so we can parse it directly
                const id = parse(doc.uuidv7);

                if(id === undefined) {
                    throw new Error("Invalid UUIDv7");
                }

                const tf = doc_tf_entries[token];
                const len = token_count;
                const tf_indexed = this.bm25Static(tf, len, averageDocumentLength);

                const entry : TermFrequencyEntry = {
                    pk: `${this.indexName}#${token}`, //partition key
                    id: id, //sort key UUIDv7
                    tf: tf_indexed, //term frequency
                }

                term_frequency_entries.push(entry);
            }
        }

        //create the IDF entries for each token
        for(const token in invertedIndexIDFEntries)
        {
            const idf = invertedIndexIDFEntries[token].size;
            const id = UUID_000; //placeholder for the IDF entry sort key
            const idf_entry : IndexEntry = {
                pk: `${this.indexName}#${token}`, //partition key
                id: id, //sort key placeholder for idf
                idf: idf //inverse document frequency
            }
            idf_entries.push(idf_entry);
        }

        const global_stats_entry : GlobalStatisticsEntry = {
            pk: `${this.indexName}#global_stats`, //partition key
            id: UUID_000, //sort key placeholder for global stats
            totalDocumentLength: totalDocumentLength,
            documentCount: documents.length
        };

        return {
            global_stats_entry: global_stats_entry,
            idf_entries: idf_entries,
            tf_entries: term_frequency_entries
        };
    }
    
}