import { GlobalStatisticsEntry, IndexedDocument, IndexEntry, InverseDocumentFrequencyEntry, TermFrequencyEntry, UUID_000 } from "../FeatherTypes";
import { BM25Score } from "../FeatherTypes";
import { expandQueryToTokens } from "./NLPUtils";
import PromisePool from "@supercharge/promise-pool";
import { pack_tf_binary, unpack_tf_binary } from "./BinaryUtils";
import { stringify, parse as unpack_uuid_binary } from "uuid";

export abstract class FeatherBMIndex
{
    //change this depending on your DB API rate limits and core count
    MAX_CONCURRENT_QUERIES = 4;

    //BM25 parameters
    //Want to understand the significance of the params below?: https://www.youtube.com/watch?v=ruBm9WywevM
    K1 = 1.2;
    B = 0.75;

    indexName: string;
    //the count in tokens of all documents added to the index
    totalDocumentLength: number;
    documentCount: number;

    //Your adapter must implement these methods to interact with your data store
    abstract getEntries(token: string) : Promise<{ idf_entry: InverseDocumentFrequencyEntry, tf_entries: TermFrequencyEntry[] }>;
    
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

    async getAverageDocumentLength(): Promise<number> {
        return this.totalDocumentLength / this.documentCount;
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

        //Adapter is responsible for updating the global stats entry in the data store
        await this.update_global_entry_internal(new_global_stats_entry);
    }
        

    //NOTE: you should ensure the documents are NOT already in the index before calling insert
    //calling insert here will overwrite any existing documents with the same id but it might result in undefined index behavior
    //if you inserted 2 different versions of the same document with the same UUID then the index will keep both versions which you may not want
    async insert(documents: IndexedDocument[] | IndexedDocument) : Promise<void>{
        if(!Array.isArray(documents)) documents = [documents]; //ensure we have an array of documents
        if(documents.length === 0) return; //nothing to insert
        const { global_stats_entry, idf_entries, tf_entries } = this.computeInvertedEndexEntries(documents);

        //Adapter is responsible for inserting the entries into the data store
        await this.insert_internal(tf_entries, idf_entries);

        //update the totalDocumentLength and documentCount
        this.totalDocumentLength += global_stats_entry.totalDocumentLength;
        this.documentCount += global_stats_entry.documentCount;

        //update the global stats entry in the data store
        await this.updateGlobalEntry(global_stats_entry, true); //true = insert
    }

    //NOTE: you should ensure the documents are already in the index before calling delete
    async delete(documents: IndexedDocument[] | IndexedDocument ) : Promise<void> {
        if(!Array.isArray(documents)) documents = [documents]; //ensure we have an array of documents
        if(documents.length === 0) return; //nothing to delete
        const { global_stats_entry, idf_entries, tf_entries } = this.computeInvertedEndexEntries(documents);

        //Adapter is responsible for deleting the entries from the data store
        await this.delete_internal(tf_entries, idf_entries);

        //update the totalDocumentLength and documentCount
        this.totalDocumentLength -= global_stats_entry.totalDocumentLength;
        this.documentCount -= global_stats_entry.documentCount;

        //update the global stats entry to maintain accuracy of BM25 accross the index
        if(this.documentCount <= 0) throw new Error("Document count cannot be less than 0");
        if(this.totalDocumentLength <= 0) throw new Error("Total document length cannot be less than 0");
        const new_global_stats_entry : GlobalStatisticsEntry = {
            pk: `${this.indexName}#global_stats`, //partition key
            id: UUID_000, //sort key placeholder for global stats
            totalDocumentLength: this.totalDocumentLength,
            documentCount: this.documentCount
        };

        //update the global stats entry in the data store
        await this.updateGlobalEntry(new_global_stats_entry, false); //false = delete
    }

    //compute the BM25 scores for every relevant document in our inverted index for a given query CONCURRENTLY
    async query(query: string) : Promise<BM25Score[]> { 
        const query_tokens = expandQueryToTokens(query);

        //split the query into tokens and get the BM25 scores for each token concurrently
        // this computes BM25 for each document in the tokens lookup table
        const { results } = await PromisePool
        .withConcurrency(this.MAX_CONCURRENT_QUERIES)
        .for(query_tokens)
        .handleError(async (error, token, pool) => {
            console.error("Error processing token index lookup", { error });
        })
        .process(async (token, index, pool) => await this.queryToken(token));
        
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
        return scores.sort((a, b) => b.score - a.score);
    }

    //query the inverted index for a single token and compute the BM25 scores for all documents that contain that token
    //this is called by the query function above for each token in the query
    //it returns an array of BM25 scores for each document that contains the token
    async queryToken(token: string) : Promise<BM25Score[]> {
        const averageDocumentLength = await this.getAverageDocumentLength();

        const { idf_entry, tf_entries } = await this.getEntries(token);
        // Token not found in inverted index
        if(tf_entries === undefined || tf_entries === null || tf_entries.length === 0) return [];

        //split out the idf entry from the rest of the entries
        //IDF Entry is the first entry in the list
        if(idf_entry === undefined || idf_entry.idf === undefined || idf_entry.id !== UUID_000) throw new Error(`No IDF entry found for token: ${token}`);
        
        //compute the BM25 score for each document in the inverted index for this token
        const scores : BM25Score[] = tf_entries.map(entry => {
            const { tf, len, timestamp } = unpack_tf_binary(entry.tf);
            const uuid = stringify(entry.id);
            const score = this.bm25(idf_entry.idf, tf, len, averageDocumentLength);
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

    //the function that actually builds the inverted index entries for a set of documents
    computeInvertedEndexEntries(documents: IndexedDocument[])
    {
        const term_frequency_entries : TermFrequencyEntry[] = [];
        const idf_entries : InverseDocumentFrequencyEntry[] = [];
        if(documents.length === 0) throw new Error("No documents to index");
        if(documents.some(doc => !doc.uuidv7)) throw new Error("All documents must have a uuidv7");

        const invertedIndexIDFEntries : { [token: string] : Set<string> } = {};
        var totalDocumentLength = 0;

        //TODO: make this a concurrent operation
        for(const doc of documents)
        {
            const words = expandQueryToTokens(doc.full_text);
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
                const id = unpack_uuid_binary(doc.uuidv7);

                const tf = doc_tf_entries[token];
                const len = token_count;
                const timestamp = id.slice(0, 6); //first 6 bytes of the UUIDv7
                const tf_binary = pack_tf_binary(tf, len, timestamp);

                const entry : TermFrequencyEntry = {
                    pk: `${this.indexName}#${token}`, //partition key
                    id: id, //sort key UUIDv7
                    tf: tf_binary //12 byte secondary index sort key
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