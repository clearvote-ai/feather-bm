//the flat inverted index is an array of values to model the DynamoDB Table
export type InvertedIndexFlat = InvertedIndexEntry[]  
export type InvertedIndex = { [ token: string ] : InvertedIndexEntry }

//maps [token] -> { [doc_id]: termFrequency }
export type InvertedIndexEntryFlat = { token: string, doc_id: string, termFrequency: number, document_token_count: number }
export type InvertedIndexEntry = { documents: { [doc_id: string]: number }, inverseDocumentFrequency: number }

export interface QueryStats {
    inverted_index_gets: number;
    time_taken_in_ms?: number;
}

export interface BM25Score {
    doc_id: string,
    score: number
}


export interface DocumentIndex
{
    invertedIndex: InvertedIndex,
    document_token_counts: { [ doc_id: string ] : number }
    averageDocumentLength: number,
    documentCount: number 
}

export interface IndexedDocument 
{
    sortkey: string,
    full_text: string 
}