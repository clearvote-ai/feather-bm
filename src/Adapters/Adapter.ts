import { BM25Score, IndexedDocument, InvertedIndexEntry } from "../BM25/InvertedIndex";
import { computeBM25ScoresConcurrent } from "../BM25/Search";

export abstract class FeatherBMIndex
{
    abstract getEntry(token: string) : Promise<InvertedIndexEntry | undefined>;
    abstract getAverageDocumentLength() : Promise<number>;

    async query(query: string) : Promise<BM25Score[]> { return await computeBM25ScoresConcurrent(query, this); }
    abstract insert(document: IndexedDocument) : Promise<void>;
    abstract insert_batch(documents: IndexedDocument[]) : Promise<void>;
    abstract delete(sortkey: string) : Promise<void>;

}