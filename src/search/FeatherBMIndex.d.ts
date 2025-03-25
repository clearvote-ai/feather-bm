//Default UUID for IDF and Global stats entries
export declare const UUID_000: Uint8Array;
export type UUID_000 = typeof UUID_000;

//we need these fields to be able to compute a BM25 score : inverseDocumentFrequency: number, termFrequency: number, documentLength: number, averageDocumentLength: number
export type InvertedIndex = IndexEntry[];
type IndexToken = string;
type IndexID = string;
//partition key for the search table is the `index_name#token`
//NOTE: basically you could jam whatever UTF8 string you want in IndexID
//smaller is better for query performance and cost
export type IndexPartitionKey = `${IndexID}#${IndexToken}`;

export type TermFrequencyEntry = {
    pk: IndexPartitionKey, //partition key
    id: Uint8Array, //sort key UUIDv7
    tf: Uint8Array, //6 byte secondary index sort key:
    // first 2 bytes are term frequency, 16 bit uint
    // next 4 bytes are document len, 32 bit uint
}

//we also need a global statistics object to keep up with the averageDocumentLength and the document_token_counts
export type GlobalStatisticsEntry = {
    pk: `${IndexID}#global_stats`, //partition key
    id: UUID_000, //sort key placeholder for global stats
    totalDocumentLength: number,
    documentCount: number,
}

export type InverseDocumentFrequencyEntry = {
    pk: IndexPartitionKey, //partition key
    id: UUID_000, //sort key placeholder for idf = 000_UUID
    idf: number //inverse document frequency
}

export type IndexEntry = TermFrequencyEntry | InverseDocumentFrequencyEntry;

export type BM25Score ={
    id: string,
    score: number
}