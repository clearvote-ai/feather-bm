import { InverseDocumentFrequencyEntry, TermFrequencyEntry, GlobalStatisticsEntry } from "../FeatherBMIndex.d";
import { FeatherBMIndex, UUID_000 } from "../FeatherBMIndex";
import BTree from "sorted-btree";
import { stringify } from "uuid";
import { IngestionDocument } from "../../documents/FeatherDocumentStore.d";

export class HashIndex extends FeatherBMIndex
{

    index: { [pk: string]: BTree<string, TermFrequencyEntry | InverseDocumentFrequencyEntry> } = {}
    global_entry : GlobalStatisticsEntry = { 
        pk: `${this.indexName}#global_stats`, 
        id: UUID_000, 
        totalDocumentLength: this.getAverageDocumentLength(), 
        documentCount: this.documentCount 
    }

    constructor(indexName: string, averageDocumentLength: number, documentCount: number)
    {
        super(indexName, averageDocumentLength, documentCount);
    }

    static async from(docs: IngestionDocument[], index_name: string = "test_index"): Promise<HashIndex> {
        const index = new HashIndex(index_name, 0, 0);
        await index.insert(docs);
        return index;
    }

    getEntries(token: string): Promise<{ idf_entry: InverseDocumentFrequencyEntry; tf_entries: TermFrequencyEntry[]; }> {
        const pk = `${this.indexName}#${token}`;
        const idf = this.index[pk].get(stringify(UUID_000)) as InverseDocumentFrequencyEntry;
        const tf_iterator = this.index[pk].values() as IterableIterator<TermFrequencyEntry>;
        const tf_array = Array.from(tf_iterator).filter((entry) => entry.id !== UUID_000);
        return Promise.resolve({ idf_entry: idf, tf_entries: tf_array });
    }

    update_global_entry_internal(global_stats: GlobalStatisticsEntry): Promise<void> {
        this.global_entry = global_stats;
        return Promise.resolve()
    }

    insert_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[]): Promise<void> {
        for (const entry of tf_entries) {
            this.insert_into_index(entry);
        }
        for (const entry of idf_entries) {
            this.insert_into_index(entry);
        }
        return Promise.resolve();
    }

    delete_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[]): Promise<void> {
        for (const entry of tf_entries) {
            this.delete_from_index(entry);
        }
        for (const entry of idf_entries) {
            this.delete_from_index(entry);
        }
        return Promise.resolve();
    }

    insert_into_index(entry: TermFrequencyEntry | InverseDocumentFrequencyEntry): void 
    {
        //if the pk doesn't exist, create it
        if (!this.index[entry.pk]) {
            this.index[entry.pk] = new BTree(
                undefined,
                (a: string, b: string) => a.localeCompare(b),
            );
        }
        
        const uuid = stringify(entry.id);
        this.index[entry.pk].set(uuid, entry);
    }

    delete_from_index(entry: TermFrequencyEntry | InverseDocumentFrequencyEntry): void {
        const uuid = stringify(entry.id);
        this.index[entry.pk].delete(uuid);
    }

}