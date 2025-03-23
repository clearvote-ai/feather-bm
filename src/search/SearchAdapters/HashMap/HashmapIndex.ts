import { IndexedDocument, InvertedIndex, InvertedIndexGlobalStatistics, IndexEntry } from "../../../FeatherTypes";
import { buildInvertedEndexEntries } from "../../BM25/InvertedIndex";
import { FeatherBMIndex } from "../FeatherBMIndex";




export class HashMapIndex extends FeatherBMIndex
{
    index: InvertedIndex = [];

    constructor(docs: IndexedDocument[], indexName: string)
    {
        if(docs.length === 0) throw new Error("No documents to index");
        const { global_stats, index } = buildInvertedEndexEntries(docs, indexName);

        super(indexName, global_stats.totalDocumentLength, global_stats.documentCount);
        this.index = index;
    }

    getEntry(token: string): Promise<IndexEntry | undefined> {
        return Promise.resolve(this.index[token]);
    }

    async getAverageDocumentLength(): Promise<number> {
        return this.totalDocumentLength / this.documentCount
    }

    async insert_internal(index: InvertedIndex, global_stats: InvertedIndexGlobalStatistics): Promise<void> {
        
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
    
    async delete_internal(minus_index: InvertedIndex, minus_global_stats: InvertedIndexGlobalStatistics): Promise<void> {
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