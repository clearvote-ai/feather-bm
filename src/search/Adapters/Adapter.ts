import { BM25Score, buildInvertedEndexEntries, IndexedDocument, InvertedIndex, InvertedIndexEntry, InvertedIndexGlobalStatistics } from "../BM25/InvertedIndex";
import { queryConcurrent } from "../BM25/query";

export abstract class FeatherBMIndex
{
    abstract getEntry(token: string) : Promise<InvertedIndexEntry | undefined>;
    abstract getAverageDocumentLength() : Promise<number>;

    async query(query: string) : Promise<BM25Score[]> { return await queryConcurrent(query, this); }
    async insert(document: IndexedDocument) : Promise<void> { await this.insert_batch([document]); }
    async delete(document: IndexedDocument) : Promise<void> { await this.delete_batch([document]); }

    async insert_batch(documents: IndexedDocument[]) : Promise<void>{
        //TODO: check if the document id already exists in the index
        const { index , global_stats } = buildInvertedEndexEntries(documents);
        await this.insert_batch_internal(index, global_stats);
    }

    async delete_batch(documents: IndexedDocument[]) : Promise<void> {
        //TODO: check if the document id exists in the index
        const { index , global_stats } = buildInvertedEndexEntries(documents);
        await this.delete_batch_internal(index, global_stats);
    }
    
    abstract insert_batch_internal(index: InvertedIndex, global_stats: InvertedIndexGlobalStatistics) : Promise<void>;
    abstract delete_batch_internal(index: InvertedIndex, global_stats: InvertedIndexGlobalStatistics) : Promise<void>;

}