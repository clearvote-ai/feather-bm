import { InvertedIndexEntry, IndexedDocument, buildInvertedEndexEntries, InvertedIndex, InvertedIndexGlobalStatistics } from "../BM25/InvertedIndex";
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

    async getAverageDocumentLength(): Promise<number> {
        return this.totalDocumentLength / this.documentCount
    }

    async insert_batch_internal(index: InvertedIndex, global_stats: InvertedIndexGlobalStatistics): Promise<void> {
        
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
    }
    
    async delete_batch_internal(minus_index: InvertedIndex, minus_global_stats: InvertedIndexGlobalStatistics): Promise<void> {
        //remove from this.index all the entries in minus_index
        for(const token in minus_index)
        {
            if(this.index[token] === undefined) continue;

            const minus_docs = minus_index[token].documents;
            const docs = this.index[token].documents;

            //filter in document id rather than idf or len
            this.index[token].documents = docs.filter(doc => !minus_docs.some(minus_doc => minus_doc.id === doc.id));
        }

        this.totalDocumentLength -= minus_global_stats.totalDocumentLength;
        this.documentCount -= minus_global_stats.documentCount;
    }


}