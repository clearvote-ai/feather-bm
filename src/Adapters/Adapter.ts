import { BM25Score, InvertedIndexEntry } from "../BM25/InvertedIndex";
import { computeBM25ScoresConcurrent } from "../BM25/Search";

export abstract class FeatherBMIndex
{
    abstract getEntry(token: string) : Promise<InvertedIndexEntry | undefined>;
    abstract getAverageDocumentLength() : Promise<number>;

    async query(query: string) : Promise<BM25Score[]> { return await computeBM25ScoresConcurrent(query, this); }
    abstract insert_document(sortkey: string, full_text: string) : Promise<void>;
    abstract delete_document(sortkey: string) : Promise<void>;

}