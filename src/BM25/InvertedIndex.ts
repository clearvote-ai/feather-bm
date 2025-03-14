import { expandQueryToTokens } from "./NLPUtils"

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
    averageDocumentLength: number,
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

export function buildInvertedEndexEntries(documents: IndexedDocument[]) : InvertedIndex
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

    return invertedIndex;
}

/*
//build the inverted index from documents passed in and return an DocumentIndex object to be saved to S3
export function buildInvertedIndexLegacy(documents: IndexedDocument[]) : {
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

}*/