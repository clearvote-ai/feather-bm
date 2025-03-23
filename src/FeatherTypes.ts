//Default UUID for IDF and Global stats entries
export const UUID_000 = new Uint8Array(16);
export type UUID_000 = typeof UUID_000;

export interface IndexedDocument 
{
    uuidv7: string, //must be a uuidv7
    title?: string,
    full_text: string 
}

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
    tf: Uint8Array, //12 byte secondary index sort key:
    // first 2 bytes are term frequency, 16 bit uint
    // next 4 bytes are document len, 32 bit uint
    // next 6 bytes are the timestamp (first 6 bytes copied from UUIDv7)
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

export interface QueryStats {
    inverted_index_gets: number;
    time_taken_in_ms?: number;
}

export interface BM25Score {
    id: string,
    score: number
}

export type FeatherBMQueryResult = {
    scores: BM25Score[],
    stats: QueryStats
}