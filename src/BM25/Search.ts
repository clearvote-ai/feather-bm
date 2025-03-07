import PromisePool from "@supercharge/promise-pool";
import { DocumentIndex } from "./InvertedIndex";
import { expandQueryToTokens } from "./WinkUtilsWrapper";




//compute the BM25 scores for every relevant document in our inverted index for a given query
export async function getDocumentBM25Scores(query: string, document_index: DocumentIndex) : Promise<{ index_gets : number, scores: { doc_id: number, score: number }[]}>
{
    const query_tokens = await expandQueryToTokens(query);

    var total_index_gets = 0;

    const { results : per_token_scores, errors } = await PromisePool
    .withConcurrency(5)
    .for(query_tokens)
    //export type ErrorHandler<T> = (error: Error, item: T, pool: Stoppable & UsesConcurrency) => Promise<void> | void;
    .handleError(async (error, token, pool) => {
        console.error("Error processing token index lookup", error);
    })
    .process(async (token, index, pool) => {
        const scores : { [doc_id: number]: number } = {};
        const entry = document_index.invertedIndex[token];
        const entry_count = Object.keys(entry?.value ?? {}).length;
        //DynamoDB charges one index get per 4KB of data read
        total_index_gets += Math.ceil(entry_count / 1000);


        if(!entry) {
            console.log("Token not found in inverted index: ", token);
            return {};
        }

        for(const doc_id in entry.value)
        {   
            const doc_int = parseInt(doc_id);
            if(!scores[doc_int]) scores[doc_int] = 0;
            scores[doc_int] += await computeBM25Score(token, doc_int, document_index);
        }

        return scores;
    });

    const scores : { [doc_id: number]: number } = per_token_scores.reduce((a, b) => {
        const a_keys = Object.keys(a);
        const b_keys = Object.keys(b);

        const keys = a_keys.concat(b_keys);

        const result : { [doc_id: number]: number } = {};

        keys.forEach( (key) => {
            const key_int = parseInt(key);
            result[key_int] = (a[key_int] ?? 0) + (b[key_int] ?? 0);
        });

        return result;
    });

    const final_scores = Object.keys(scores).map(string_id => {
        const doc_id = parseInt(string_id);
        return ({ doc_id, score: scores[doc_id] })
    }).sort((a, b) => b.score - a.score);

    return { index_gets: total_index_gets, scores: final_scores };
}

//compute the BM25 score for a single TOKEN_X_DOCUMENT pair
export async function computeBM25Score(token: string, document_id: number, index: DocumentIndex) : Promise<number>
{
    const entry = index.invertedIndex[token];
    if(!entry) throw new Error("Failed to get entry for token: " + token);

    //Want to understand the significance of the params below?: https://www.youtube.com/watch?v=ruBm9WywevM
    const inverseDocumentFrequency = entry.inverseDocumentFrequency;
    const termFrequency = entry.value[document_id];
    const documentLength = index.document_token_counts[document_id] ?? 0;
    const k1 = 1.2;
    const b = 0.75;

    return (
        (inverseDocumentFrequency * (termFrequency * (k1 + 1))) /
        (termFrequency +
        k1 * (1 - b + (b * documentLength) / index.averageDocumentLength))
    );
}