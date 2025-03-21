import { BM25Score, buildInvertedEndexEntries, IndexedDocument, InvertedIndex, InvertedIndexEntry, InvertedIndexGlobalStatistics } from "../BM25/InvertedIndex";
import { expandQueryToTokens } from "../BM25/NLPUtils";
import PromisePool from "@supercharge/promise-pool";

export abstract class FeatherBMIndex
{
    //change this depending on your DB API rate limits and core count
    MAX_CONCURRENT_QUERIES = 4;

    //BM25 parameters
    //Want to understand the significance of the params below?: https://www.youtube.com/watch?v=ruBm9WywevM
    K1 = 1.2;
    B = 0.75;

    abstract getEntry(token: string) : Promise<InvertedIndexEntry | undefined>;
    abstract getAverageDocumentLength() : Promise<number>;

    //compute the BM25 scores for every relevant document in our inverted index for a given query
    async query(query: string) : Promise<BM25Score[]> { 
        const query_tokens = expandQueryToTokens(query);

        const averageDocumentLength = await this.getAverageDocumentLength();

        //split the query into tokens and get the BM25 scores for each token concurrently
        // this computes BM25 for each document in the tokens lookup table
        const { results } = await PromisePool
        .withConcurrency(this.MAX_CONCURRENT_QUERIES)
        .for(query_tokens)
        .handleError(async (error, token, pool) => {
            console.error("Error processing token index lookup", { error });
        })
        .process(async (token, index, pool) => {
            
            const entry = await this.getEntry(token);

            // Token not found in inverted index, handle appropriately in production
            if(entry === undefined || entry === null) return [];
            

            //compute the BM25 score for each document in the inverted index for this token
            const scores : BM25Score[] = entry.documents.map(doc => {
                const score = this.computeBM25(entry.idf, doc.tf, doc.len, averageDocumentLength);
                return { id: doc.id, score: score };
            });

            return scores;
        });

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

    //compute the BM25 score for a single TOKEN_X_DOCUMENT pair
    computeBM25(inverseDocumentFrequency: number, termFrequency: number, documentLength: number, averageDocumentLength: number) : number
    {
        return (
            (inverseDocumentFrequency * (termFrequency * (this.K1 + 1))) /
            (termFrequency +
            this.K1 * (1 - this.B + (this.B * documentLength) / averageDocumentLength))
        );
    }
    
    async insert(document: IndexedDocument) : Promise<void> { await this.insert_batch([document]); }
    async delete(document: IndexedDocument) : Promise<void> { await this.delete_batch([document]); }

    async insert_batch(documents: IndexedDocument[]) : Promise<void>{
        //TODO: check if the document id already exists in the index
        const { index , global_stats } = buildInvertedEndexEntries(documents);
        await this.insert_batch_internal(index, global_stats);
    }

    async delete_batch(documents: IndexedDocument[]) : Promise<void> {
        //TODO: check if the document id exists in the index
        const { index , global_stats } = buildInvertedEndexEntries(documents);
        await this.delete_batch_internal(index, global_stats);
    }
    
    abstract insert_batch_internal(index: InvertedIndex, global_stats: InvertedIndexGlobalStatistics) : Promise<void>;
    abstract delete_batch_internal(index: InvertedIndex, global_stats: InvertedIndexGlobalStatistics) : Promise<void>;

}