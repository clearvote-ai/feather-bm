import { DocumentIndex, IndexedDocument, InvertedIndex } from "./BM25";
import { expandQueryToTokens } from "./WinkUtilsWrapper"


//build the inverted index from documents passed in and return an DocumentIndex object to be saved to S3
export async function buildInvertedIndex(documents: IndexedDocument[]) : Promise<DocumentIndex>
{
    const invertedIndex : InvertedIndex = {};
    let averageDocumentLength : number = 0;

    const document_token_counts : { [ sortkey: string ]: number } = {};

    for(const document of documents)
    {
        const words = await expandQueryToTokens(document.full_text);
        const token_count = words.length;
        document_token_counts[document.sortkey] = token_count;
        const uniqueWords = new Set<string>();

        for(const word of words)
        {
            if(!invertedIndex.hasOwnProperty(word)) invertedIndex[word] = {
                documents: { [ document.sortkey ]: 1}, 
                inverseDocumentFrequency: 0 
            }

            if(!invertedIndex[word].hasOwnProperty('documents')) {
                console.log("word not found in inverted index: ", JSON.stringify(invertedIndex[word])); 
                invertedIndex[word].documents = {}; 
            }

            if(!invertedIndex[word].documents.hasOwnProperty(document.sortkey)) invertedIndex[word].documents[document.sortkey] = 0;

            invertedIndex[word].documents[document.sortkey]++;

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