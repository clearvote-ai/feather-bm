import PromisePool from "@supercharge/promise-pool";
import { BM25Score, QueryStats } from "./InvertedIndex";
import { expandQueryToTokens } from "./NLPUtils";
import { FeatherBMIndex } from "../Adapters/Adapter";

//change this depending on your DB API rate limits and core count
const MAX_CONCURRENT_QUERIES = 4;

//compute the BM25 scores for every relevant document in our inverted index for a given query
export async function computeBM25ScoresConcurrent<A extends FeatherBMIndex>(query: string, document_index: A) : Promise<BM25Score[]>
{
    const query_tokens = expandQueryToTokens(query);

    const averageDocumentLength = await document_index.getAverageDocumentLength();

    //split the query into tokens and get the BM25 scores for each token concurrently
    // this computes BM25 for each document in the tokens lookup table
    const { results } = await PromisePool
    .withConcurrency(MAX_CONCURRENT_QUERIES)
    .for(query_tokens)
    .handleError(async (error, token, pool) => {
        console.error("Error processing token index lookup", error);
    })
    .process(async (token, index, pool) => {
        
        const entry = await document_index.getEntry(token);

        if(entry === undefined || entry === null) {
            console.log("Token not found in inverted index: ", token);
            return {};
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

    
    var scores : BM25Score[] = aggregatePerTokenScores(results as [BM25Score[]])
                               .sort((a, b) => b.score - a.score);

    return scores;
}

//aggregate scores across all tokens 
//BEFORE: { "token_1" : [ { doc_id: "1234", score: 0.5 }, { doc_id: "4567", score: 0.6 } ], "token_2" : [ { doc_id: "1234", score: 0.5 }, { doc_id: "4567", score: 0.6 } ] }
//AFTER: [ { doc_id: "1234", score: 1.0 }, { doc_id: "4567", score: 1.2 } ]
function aggregatePerTokenScores(scores: [BM25Score[]]) : BM25Score[]
{
    return scores.reduce((acc, val) => {
        val.forEach(score => {
            const existing_score = acc.find(s => s.id === score.id);
            if(existing_score) existing_score.score += score.score;
            else acc.push(score);
        });
        return acc;
    }, [] as BM25Score[]);
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