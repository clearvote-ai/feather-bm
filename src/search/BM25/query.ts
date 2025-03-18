import PromisePool from "@supercharge/promise-pool";
import { BM25Score, QueryStats } from "./InvertedIndex";
import { expandQueryToTokens } from "./NLPUtils";
import { FeatherBMIndex } from "../Adapters/Adapter";

//change this depending on your DB API rate limits and core count
const MAX_CONCURRENT_QUERIES = 4;

//compute the BM25 scores for every relevant document in our inverted index for a given query
export async function queryConcurrent<A extends FeatherBMIndex>(query: string, document_index: A) : Promise<BM25Score[]>
{
    const query_tokens = expandQueryToTokens(query);

    const averageDocumentLength = await document_index.getAverageDocumentLength();

    //split the query into tokens and get the BM25 scores for each token concurrently
    // this computes BM25 for each document in the tokens lookup table
    const { results } = await PromisePool
    .withConcurrency(MAX_CONCURRENT_QUERIES)
    .for(query_tokens)
    .handleError(async (error, token, pool) => {
        console.error("Error processing token index lookup", { error });
    })
    .process(async (token, index, pool) => {
        
        const entry = await document_index.getEntry(token);

        if(entry === undefined || entry === null) {
            // Token not found in inverted index, handle appropriately in production
            // e.g., use a logging library instead of console.log
            return [];
        }

        const scores : BM25Score[] = entry.documents.map(doc => {
            const termFrequency = doc.tf;
            const documentLength = doc.len;
            const inverseDocumentFrequency = entry.idf;

            return { id: doc.id, score: computeBM25Score(inverseDocumentFrequency, termFrequency, documentLength, averageDocumentLength) };
        });

        return scores;
    });

    if(!results) throw new Error("No scores found for query");

    
    const flattenedResults = results.flat();
    let scores : BM25Score[] = flattenedResults.reduce((acc, score) => {
        const existing_score = acc.find(s => s.id === score.id);
        if(existing_score) existing_score.score += score.score;
        else acc.push(score);
        return acc;
    }, [] as BM25Score[]);

    return scores.sort((a, b) => b.score - a.score);
}

//compute the BM25 score for a single TOKEN_X_DOCUMENT pair
export function computeBM25Score(inverseDocumentFrequency: number, termFrequency: number, documentLength: number, averageDocumentLength: number) : number
{
    //Want to understand the significance of the params below?: https://www.youtube.com/watch?v=ruBm9WywevM
    const k1 = 1.2;
    const b = 0.75;

    return (
        (inverseDocumentFrequency * (termFrequency * (k1 + 1))) /
        (termFrequency +
        k1 * (1 - b + (b * documentLength) / averageDocumentLength))
    );
}