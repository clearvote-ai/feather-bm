import { expandQueryToTokens } from "./NLPUtils"
import { computeBM25ScoresConcurrent } from "./Search";

//we need these fields to be able to compute a BM25 score : inverseDocumentFrequency: number, termFrequency: number, documentLength: number, averageDocumentLength: number
export type InvertedIndex = { [ token: string ] : InvertedIndexEntry }

//The InvertedIndexEntry only contains the token_x_document specific fields
export type InvertedIndexEntry = { documents: { [doc_id: string]: { termFrequency: number, documentLength: number } }, inverseDocumentFrequency: number }

//we also need a global statistics object to keep up with the averageDocumentLength and the document_token_counts
export interface InvertedIndexGlobalStatistics {
    document_token_counts: { [ doc_id: string ] : number },
    averageDocumentLength: number,
    documentCount: number
}

export interface QueryStats {
    inverted_index_gets: number;
    time_taken_in_ms?: number;
}

export interface BM25Score {
    doc_id: string,
    score: number
}

export type FeatherBMQueryResult = {
    scores: BM25Score[],
    stats: QueryStats
}


export abstract class FeatherBMIndex
{
    abstract getInvertedIndexEntry(token: string) : Promise<InvertedIndexEntry | undefined>;
    abstract getAverageDocumentLength() : Promise<number>;
    async query(query: string) : Promise<FeatherBMQueryResult>
    {
        const start_time = performance.now();
        const scores = await computeBM25ScoresConcurrent(query, this);
        const end_time = performance.now();
        const time_taken = end_time - start_time;
        return { scores, stats: { inverted_index_gets: scores.length, time_taken_in_ms: time_taken } };
    }

}

export interface IndexedDocument 
{
    sortkey: string,
    full_text: string 
}

//build the inverted index from documents passed in and return an DocumentIndex object to be saved to S3
export function buildInvertedIndex(documents: IndexedDocument[]) : {
    invertedIndex: InvertedIndex,
    document_token_counts: { [ sortkey: string ]: number },
    averageDocumentLength: number,
    documentCount: number
}
{
    const invertedIndex : InvertedIndex = {};
    let averageDocumentLength : number = 0;

    const document_token_counts : { [ sortkey: string ]: number } = {};

    for(const document of documents)
    {
        const words = expandQueryToTokens(document.full_text);
        const token_count = words.length;
        document_token_counts[document.sortkey] = token_count;
        const uniqueWords = new Set<string>();

        for(const word of words)
        {
            if(!invertedIndex.hasOwnProperty(word)) invertedIndex[word] = {
                documents: { [ document.sortkey ]: { termFrequency: 0, documentLength: token_count } },
                inverseDocumentFrequency: 0 
            }

            if(!invertedIndex[word].hasOwnProperty('documents')) {
                console.log("word not found in inverted index: ", JSON.stringify(invertedIndex[word])); 
                invertedIndex[word].documents = {}; 
            }

            if(!invertedIndex[word].documents.hasOwnProperty(document.sortkey)) invertedIndex[word].documents[document.sortkey] = { termFrequency: 0, documentLength: 0 };

            invertedIndex[word].documents[document.sortkey] = {
                termFrequency: invertedIndex[word].documents[document.sortkey].termFrequency + 1,
                documentLength: token_count
            }

            if (!uniqueWords.has(word)) {
                invertedIndex[word].inverseDocumentFrequency++;
                uniqueWords.add(word);
            }
        }
        averageDocumentLength += words.length;
    }

    averageDocumentLength = averageDocumentLength / documents.length;

    return { 
        invertedIndex, 
        document_token_counts, 
        averageDocumentLength, 
        documentCount: documents.length 
    };

}