import { InvertedIndexEntry, IndexedDocument, buildInvertedEndexEntries, InvertedIndex } from "../BM25/InvertedIndex";
import { FeatherBMIndex } from "./Adapter";




export class HashMapIndex extends FeatherBMIndex
{

    index: InvertedIndex = {};
    totalDocumentLength: number = 0;
    documentCount: number = 0;

    private constructor(index : InvertedIndex, totalDocumentLength: number, documentCount: number)
    {
        super();
        this.index = index;
        this.totalDocumentLength = totalDocumentLength;
        this.documentCount = documentCount
    }

    public static from(docs: IndexedDocument[])
    {
        const { global_stats, index } = buildInvertedEndexEntries(docs);
        return new HashMapIndex(index, global_stats.totalDocumentLength, global_stats.documentCount);
    }

    getEntry(token: string): Promise<InvertedIndexEntry | undefined> {
        return Promise.resolve(this.index[token]);
    }
    getAverageDocumentLength(): Promise<number> {
        return Promise.resolve(this.totalDocumentLength / this.documentCount);
    }
    insert_batch(documents: IndexedDocument[]): Promise<void> {
        //TODO: check if the document id already exists in the index
        const { global_stats, index } = buildInvertedEndexEntries(documents);

        //combine the new index with the existing index
        //for each token in the index
        for(const token in index)
        {
            //if the token is not in the index, add it
            if(this.index[token] === undefined)
            {
                this.index[token] = index[token];
            }
            else
            {
                //if the token is in the index, add the documents to the existing entry
                this.index[token].documents.push(...index[token].documents);
            }
        }
        this.totalDocumentLength += global_stats.totalDocumentLength;
        this.documentCount += global_stats.documentCount;
        return Promise.resolve();
    }
    delete(sortkey: string): Promise<void> {
        throw new Error("Method not implemented.");
    }


}