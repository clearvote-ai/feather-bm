import { BM25Score, GlobalStatisticsEntry, IndexEntry, InverseDocumentFrequencyEntry, TermFrequencyEntry } from "./FeatherBMIndex.d";
import { expandQueryToTokens } from "./NLPUtils";
import PromisePool from "@supercharge/promise-pool";
import { parse, stringify } from "uuid";
import { FeatherDocument, IngestionDocument } from "../documents/FeatherDocumentStore.d";

//Default UUID for IDF and Global stats entries
export const UUID_000 = new Uint8Array(16);
export type UUID_000 = typeof UUID_000;

export abstract class FeatherBMIndex
{
    //change this depending on your DB API rate limits and core count
    MAX_CONCURRENT_QUERIES = 4;

    //BM25 parameters
    //Want to understand the significance of the params below?: https://www.youtube.com/watch?v=ruBm9WywevM
    //K1 = 1.2;
    //B = 0.75;
    //avgDL = 400; //default average document length aka the MIN average document length

    DEFAULT_K1 = 1.2;
    DEFAULT_B = 0.75;
    DEFAULT_AVG_DL = 400;

    //the count in tokens of all documents added to the index
    global_entries : { [indexName: string] : GlobalStatisticsEntry } = {};

    //Your adapter must implement these methods to interact with your data store
    abstract getEntries(token: string, indexName: string, max_results?: number) : Promise<{ idf_entry: InverseDocumentFrequencyEntry, tf_entries: TermFrequencyEntry[] }>;
    abstract getEntriesGlobal(token: string, indexName: string, max_results?: number) : Promise<{ idf_entry: InverseDocumentFrequencyEntry, tf_entries: TermFrequencyEntry[] }>;
    
    abstract update_global_entry_internal(global_stats: GlobalStatisticsEntry) : Promise<void>;
    abstract get_global_entry_internal(indexName: string) : Promise<GlobalStatisticsEntry | undefined>;
    abstract insert_internal(tf_entries: TermFrequencyEntry[], idf_entries:InverseDocumentFrequencyEntry[]) : Promise<void>;
    abstract delete_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[]) : Promise<void>;

    //TODO: add a function to create a custom global entry for k1, b and avgDL

    private async retrieveGlobalEntry(indexName: string) : Promise<GlobalStatisticsEntry> {
        //check if we already have the global entry in memory
        const global_stats_entry = this.global_entries[indexName];
        if(global_stats_entry) return global_stats_entry;
        //if not, retrieve it from the data store
        const global_stats_entry_from_store = await this.get_global_entry_internal(indexName);
        if(!global_stats_entry_from_store) {
            //if the entry does not exist, create a new one
            const new_global_stats_entry : GlobalStatisticsEntry = {
                pk: `${indexName}#global_stats`, //partition key
                id: UUID_000, //sort key placeholder for global stats
                totalDocumentLength: 0,
                documentCount: 0,
                k1: this.DEFAULT_K1,
                b: this.DEFAULT_B,
                avgDL: this.DEFAULT_AVG_DL
            };
            //insert the new entry into the data store
            await this.update_global_entry_internal(new_global_stats_entry);
            //update the global stats entry in memory
            this.global_entries[indexName] = new_global_stats_entry;
            return new_global_stats_entry;
        }
        //update the global stats entry in memory
        this.global_entries[indexName] = global_stats_entry_from_store;
        return global_stats_entry_from_store;
    }

    async updateGlobalEntry(global_stats: GlobalStatisticsEntry, insert: boolean, indexName: string) : Promise<void> {
        
        const global_stats_entry = this.global_entries[indexName];
        var documentCount = global_stats_entry.documentCount;
        var totalDocumentLength = global_stats_entry.totalDocumentLength;
        if(!global_stats_entry) throw new Error("Global stats entry not found");

        const new_document_count = insert ? documentCount + global_stats.documentCount : documentCount - global_stats.documentCount;
        const new_total_document_length = insert ? totalDocumentLength + global_stats.totalDocumentLength : totalDocumentLength - global_stats.totalDocumentLength;
        if(new_document_count <= 0) throw new Error("Document count cannot be less than 0");
        if(new_total_document_length <= 0) throw new Error("Total document length cannot be less than 0");
        const new_global_stats_entry : GlobalStatisticsEntry = {
            pk: `${indexName}#global_stats`, //partition key
            id: UUID_000, //sort key placeholder for global stats
            totalDocumentLength: new_total_document_length,
            documentCount: new_document_count,
            k1: global_stats_entry.k1,
            b: global_stats_entry.b,
            avgDL: global_stats_entry.avgDL
        };

        //update the global stats entry in memory
        this.global_entries[indexName] = new_global_stats_entry;

        //Adapter is responsible for updating the global stats entry in the data store
        await this.update_global_entry_internal(new_global_stats_entry);
    }
        

    //NOTE: you should ensure the documents are NOT already in the index before calling insert
    //calling insert here will overwrite any existing documents with the same id but it might result in undefined index behavior
    //if you inserted 2 different versions of the same document with the same UUID then the index will keep both versions which you may not want
    async insert(documents: FeatherDocument[] | FeatherDocument, indexName: string) : Promise<void>{
        if(!Array.isArray(documents)) documents = [documents]; //ensure we have an array of documents
        if(documents.length === 0) return; //nothing to insert

        const current_global_stats_entry = await this.retrieveGlobalEntry(indexName); //get the global stats entry for this index
        const { global_stats_entry, idf_entries, tf_entries } = this.computeInvertedEndexEntries(documents, indexName, current_global_stats_entry);

        //Adapter is responsible for inserting the entries into the data store
        await this.insert_internal(tf_entries, idf_entries);

        //update the global stats entry in the data store
        await this.updateGlobalEntry(global_stats_entry, true, indexName); //true = insert
    }

    //NOTE: you should ensure the documents are already in the index before calling delete
    async delete(documents: FeatherDocument[] | FeatherDocument, indexName: string) : Promise<void> {
        const current_global_stats_entry = await this.retrieveGlobalEntry(indexName); //get the global stats entry for this index
        if(!Array.isArray(documents)) documents = [documents]; //ensure we have an array of documents
        if(documents.length === 0) return; //nothing to delete
        const { global_stats_entry, idf_entries, tf_entries } = this.computeInvertedEndexEntries(documents, indexName, current_global_stats_entry);

        //Adapter is responsible for deleting the entries from the data store
        await this.delete_internal(tf_entries, idf_entries);

        //update the global stats entry in the data store
        await this.updateGlobalEntry(global_stats_entry, false, indexName); //false = delete
    }

    //compute the BM25 scores for every relevant document in our inverted index for a given query CONCURRENTLY
    async query(query: string, indexName: string, global: boolean = false, max_results?: number) : Promise<BM25Score[]> { 
        await this.retrieveGlobalEntry(indexName); //get the global stats entry for this index
        const query_tokens = expandQueryToTokens(query);

        //split the query into tokens and get the BM25 scores for each token concurrently
        // this computes BM25 for each document in the tokens lookup table
        const { results } = await PromisePool
        .withConcurrency(this.MAX_CONCURRENT_QUERIES)
        .for(query_tokens)
        .handleError(async (error, token, pool) => {
            console.error("Error processing token index lookup", { error });
        })
        .process(async (token, index, pool) => await this.queryToken(token, indexName, global, max_results));
        
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
    private async queryToken(token: string, indexName: string, global: boolean, max_results?: number) : Promise<BM25Score[]> {

        const { idf_entry, tf_entries } = global ? await this.getEntriesGlobal(token, indexName, max_results) : await this.getEntries(token, indexName, max_results) 
        // Token not found in inverted index
        if(tf_entries === undefined || tf_entries === null || tf_entries.length === 0) return [];

        //split out the idf entry from the rest of the entries
        //IDF Entry is the first entry in the list
        if(idf_entry === undefined || idf_entry.idf === undefined || idf_entry.id !== UUID_000) throw new Error(`No IDF entry found for token: ${token}`);
        
        //IDF(t) = log(N / df(t))
        const log_idf = Math.log(this.global_entries[indexName].documentCount / idf_entry.idf);

        //compute the BM25 score for each document in the inverted index for this token
        const scores : BM25Score[] = tf_entries.map(entry => {
            const tf_indexed = entry.tf;
            const uuid = stringify(entry.id);
            const score = tf_indexed * log_idf;
            return { id: uuid, score: score };
        });

        return scores;
    }

    //compute the BM25 score for a single TOKEN_X_DOCUMENT pair
    bm25(
        inverseDocumentFrequency: number, 
        termFrequency: number, 
        documentLength: number, 
        avgDL: number,
        K1: number,
        B: number
    ) : number
    {
        return (
            (inverseDocumentFrequency * (termFrequency * (K1 + 1))) /
            (termFrequency + K1 * (1 - B + (B * documentLength) / avgDL))
        );
    }

    bm25Static(termFrequency: number, documentLength: number, avgDL: number, K1: number, B: number) : number
    {
        return (
            (termFrequency * (K1 + 1)) / (termFrequency + K1 * (1 - B + (B * documentLength) / avgDL))
        );
    }

    //the function that actually builds the inverted index entries for a set of documents
    computeInvertedEndexEntries(documents: FeatherDocument[], indexName: string, current_global_stats_entry: GlobalStatisticsEntry)
    {
        const term_frequency_entries : TermFrequencyEntry[] = [];
        const idf_entries : InverseDocumentFrequencyEntry[] = [];
        if(documents.length === 0) throw new Error("No documents to index");
        if(documents.some(doc => !doc.id)) throw new Error("All documents must have a uuidv7");

        const invertedIndexIDFEntries : { [token: string] : Set<string> } = {};
        var totalDocumentLength = 0;
        const averageDocumentLength = current_global_stats_entry.avgDL;

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
                invertedIndexIDFEntries[token].add(doc.id);
                if(!doc_tf_entries.hasOwnProperty(token)) doc_tf_entries[token] = 0;
                doc_tf_entries[token]++;
            }

            totalDocumentLength += token_count;
            for(const token in doc_tf_entries)
            {
                //we assume that the id is already a UUIDv7, so we can parse it directly
                const id = parse(doc.id);

                if(id === undefined) {
                    throw new Error("Invalid UUIDv7");
                }

                const tf = doc_tf_entries[token];
                const len = token_count;
                const tf_indexed = this.bm25Static(tf, len, averageDocumentLength, current_global_stats_entry.k1, current_global_stats_entry.b);

                const entry : TermFrequencyEntry = {
                    pk: `${indexName}#${token}`, //partition key
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
                pk: `${indexName}#${token}`, //partition key
                id: id, //sort key placeholder for idf
                idf: idf //inverse document frequency
            }
            idf_entries.push(idf_entry);
        }

        const global_stats_entry : GlobalStatisticsEntry = {
            pk: `${indexName}#global_stats`, //partition key
            id: UUID_000, //sort key placeholder for global stats
            totalDocumentLength: totalDocumentLength,
            documentCount: documents.length,
            k1: current_global_stats_entry.k1,
            b: current_global_stats_entry.b,
            avgDL: totalDocumentLength / documents.length
        };

        return {
            global_stats_entry: global_stats_entry,
            idf_entries: idf_entries,
            tf_entries: term_frequency_entries
        };
    }
    
}