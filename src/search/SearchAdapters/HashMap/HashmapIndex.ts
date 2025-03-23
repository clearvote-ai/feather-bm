import { IndexedDocument, InvertedIndex, IndexEntry, GlobalStatisticsEntry, InverseDocumentFrequencyEntry, TermFrequencyEntry, IndexPartitionKey } from "../../../FeatherTypes";
import { FeatherBMIndex } from "../FeatherBMIndex";


export class HashMapIndex extends FeatherBMIndex
{
    
    index: Map<IndexPartitionKey, InvertedIndex> = new Map();

    static async from(docs: IndexedDocument[], indexName: string): Promise<HashMapIndex>
    {
        const index = new HashMapIndex(indexName, 0, 0);
        await index.insert(docs);
        return index;
    }

    getEntries(token: string): Promise<{ idf_entry: InverseDocumentFrequencyEntry; tf_entries: TermFrequencyEntry[]; }> {
        throw new Error("Method not implemented.");
    }

    insert_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[], global_stats: GlobalStatisticsEntry): Promise<void> {
        throw new Error("Method not implemented.");
    }

    delete_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[], global_stats: GlobalStatisticsEntry): Promise<void> {
        throw new Error("Method not implemented.");
    }
}

