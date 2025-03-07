import { expandQueryToTokens } from "./WinkUtilsWrapper"



export type InvertedIndex = { [word: string] : 
{ 
    value : { [doc_id: number]: number }
    inverseDocumentFrequency: number,
}}  

export interface DocumentIndex
{
    invertedIndex: InvertedIndex,
    document_token_counts: { [doc_id: number]: number }
    averageDocumentLength: number,
    documentCount: number,
    IDToUUIDMap: { [doc_id: number]: string }
    UUIDToIDMap: { [uuid: string]: number }
    //TODO: add a document uuid -> to simple doc number mapping to save space
}

export interface IndexedDocument {
    sortkey: string,
    full_text: string
}


//build the inverted index from documents passed in and return an DocumentIndex object to be saved to S3
export async function buildInvertedIndex(documents: IndexedDocument[]) : Promise<DocumentIndex>
{
    const invertedIndex : InvertedIndex = {};
    const documentLengths : { [doc_id: number]: number } = {};
    var averageDocumentLength : number = 0;

    const document_token_counts : { [doc_id: number]: number } = {};

    //build the UUIDToIDMap then the IDToUUIDMap
    const UUIDToIDMap : { [x: string]: number } = documents.map((doc, index) => ({ [doc.sortkey]: index })).reduce((a, b) => ({ ...a, ...b }));
    const IDToUUIDMap : { [x: number]: string } = documents.map((doc, index) => ({ [index]: doc.sortkey })).reduce((a, b) => ({ ...a, ...b }));

    var i = 0;

    for(const document of documents)
    {
        const doc_id = UUIDToIDMap[document.sortkey];
        const words = await expandQueryToTokens(document.full_text);
        const token_count = words.length;
        document_token_counts[doc_id] = token_count;
        for(const word of words)
        {
            if(!invertedIndex.hasOwnProperty(word)) invertedIndex[word] = { 
                value: { [doc_id]: 1}, 
                inverseDocumentFrequency: 0
            };

            if(!invertedIndex[word].hasOwnProperty('value')) {
                console.log("Value not found in inverted index: ", JSON.stringify(invertedIndex[word]));
                invertedIndex[word].value = {};
            }

            if(!invertedIndex[word].value.hasOwnProperty(doc_id)) invertedIndex[word].value[doc_id] = 0;
            invertedIndex[word].value[doc_id]++;
            invertedIndex[word].inverseDocumentFrequency++;
        }

        documentLengths[doc_id] = words.length;
        averageDocumentLength += words.length;
        i++;
    }

    return { 
        invertedIndex, 
        document_token_counts, 
        averageDocumentLength: averageDocumentLength / documents.length, 
        documentCount: documents.length,
        IDToUUIDMap,
        UUIDToIDMap 
    };

}