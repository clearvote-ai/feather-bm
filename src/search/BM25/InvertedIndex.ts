import { expandQueryToTokens } from "./NLPUtils";

export type InvertedIndexToken = string;
export type InvertedIndexTimeStamp = string;
//TODO: combine token and timestamp into one sortkey
export type InvertedIndexEntrySortKey = `${InvertedIndexToken}`;

//we need these fields to be able to compute a BM25 score : inverseDocumentFrequency: number, termFrequency: number, documentLength: number, averageDocumentLength: number
export type InvertedIndex = { [ sortkey: string ] : InvertedIndexEntry }

//The InvertedIndexEntry only contains the token_x_document specific fields
export type InvertedIndexEntry = { documents: InverseDocumentValue[], idf: number, }

export type InverseDocumentValue = { id: string, tf: number, len: number }

//we also need a global statistics object to keep up with the averageDocumentLength and the document_token_counts
export interface InvertedIndexGlobalStatistics {
    totalDocumentLength: number,
    documentCount: number
}

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

export interface IndexedDocument 
{
    sortkey: string,
    full_text: string 
}


function getInverseDocumentValues(document: IndexedDocument) : { [token: string] : InverseDocumentValue }
{
    const words = expandQueryToTokens(document.full_text);
    const token_count = words.length;
    const uniqueWords = new Set<string>();

    const values : { [token: string] : InverseDocumentValue } = {};

    for(const word of words)
    {
        if(!values.hasOwnProperty(word)) values[word] = { id: document.sortkey, tf: 0, len: token_count };
        values[word].tf++;

        if (!uniqueWords.has(word)) {
            uniqueWords.add(word);
        }
    }

    return values;
}

export function buildInvertedEndexEntries(documents: IndexedDocument[]) : { global_stats: InvertedIndexGlobalStatistics, index: InvertedIndex }
{
    const invertedIndex : InvertedIndex = {};

    for(const document of documents)
    {
        const values = getInverseDocumentValues(document);

        for(const token in values)
        {
            if(!invertedIndex.hasOwnProperty(token)) invertedIndex[token] = { documents: [], idf: 0 };
            invertedIndex[token].documents.push(values[token]);
            invertedIndex[token].idf++;
        }
    }

    return {
        global_stats: {
            totalDocumentLength: documents.reduce((acc, doc) => acc + expandQueryToTokens(doc.full_text).length, 0),
            documentCount: documents.length
        },
        index: invertedIndex
    };
}