import PromisePool from "@supercharge/promise-pool";
import { BM25Score, DocumentIndex, QueryStats } from "./BM25";
import { expandQueryToTokens } from "./WinkUtilsWrapper";

//compute the BM25 scores for every relevant document in our inverted index for a given query
export async function computeBM25ScoresConcurrent(query: string, document_index: DocumentIndex) : Promise<{ stats : QueryStats, scores: BM25Score[]}>
{
    const start_time = performance.now();
    const query_tokens = await expandQueryToTokens(query);

    const { results : per_token_scores_and_stats, errors } = await PromisePool
    .withConcurrency(4)
    .for(query_tokens)
    .handleError(async (error, token, pool) => {
        console.error("Error processing token index lookup", error);
    })
    .process(async (token, index, pool) => {
        var query_stats : QueryStats = {
            inverted_index_gets: 0,
        };


        const entry = document_index.invertedIndex[token];
        const entry_count = Object.keys(entry.documents ?? {}).length;
        //DynamoDB charges one index get per 4KB of data read
        query_stats.inverted_index_gets += Math.ceil(entry_count / 1000);

        if(!entry) {
            console.log("Token not found in inverted index: ", token);
            return {};
        }

        const scores : { [doc_id: string]: number } = {};

        const document_ids = Object.keys(entry.documents ?? {});

        for(const doc_id of document_ids)
        {
            const termFrequency = entry.documents[doc_id];
            const documentLength = document_index.document_token_counts[doc_id];
            const inverseDocumentFrequency = entry.inverseDocumentFrequency;

            scores[doc_id] = computeBM25Score(inverseDocumentFrequency, termFrequency, documentLength, document_index.averageDocumentLength);
        }

        return { scores, query_stats };
    });

    const scores : { [doc_id: string]: number } | undefined = per_token_scores_and_stats.map(full => full.scores).reduce((a, b) => {
        const a_scores = a ?? {};
        const b_scores = b ?? {};

        const a_keys = Object.keys(a_scores);
        const b_keys = Object.keys(b_scores);

        const keys = a_keys.concat(b_keys);

        const result : { [doc_id: string]: number } = {};

        //sum the scores for each document
        keys.forEach( (doc_id) => {
            result[doc_id] = (a_scores[doc_id] ?? 0) + (b_scores[doc_id] ?? 0);
        });

        return result;
    });

    //agregate the query stats across all threads
    const total_query_stats : QueryStats | undefined = per_token_scores_and_stats.map(full => full.query_stats).reduce((a, b) => {
        return {
            inverted_index_gets: (a?.inverted_index_gets ?? 0) + (b?.inverted_index_gets ?? 0),
        } as QueryStats;
    });

    if(!scores) throw new Error("Failed to get scores for query: " + query);

    const final_scores = Object.keys(scores).map(doc_id => {
        return ({ doc_id, score: scores[doc_id] })
    }).sort((a, b) => b.score - a.score);

    if(!total_query_stats) throw new Error("Failed to get total query stats for query: " + query);

    const end_time = performance.now();

    //add the time taken to the stats
    total_query_stats.time_taken_in_ms = end_time - start_time;

    return { stats: total_query_stats, scores: final_scores };
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