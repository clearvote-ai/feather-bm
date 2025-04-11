import { InverseDocumentFrequencyEntry, TermFrequencyEntry, GlobalStatisticsEntry } from "../FeatherBMIndex.d";
import { FeatherBMIndex, UUID_000 } from "../FeatherBMIndex";
import BTree from "sorted-btree";
import { stringify } from "uuid";
import { FeatherDocument, IngestionDocument } from "../../documents/FeatherDocumentStore.d";

export class HashIndex extends FeatherBMIndex
{
    
    index: { [pk: string]: BTree<string, TermFrequencyEntry | InverseDocumentFrequencyEntry> } = {}
    tf_global_index : { [pk: string]: BTree<number, TermFrequencyEntry> } = {}

    global_entry_store: { [pk: string]: GlobalStatisticsEntry } = {}

    static async from(docs: FeatherDocument[], index_name: string): Promise<HashIndex> {
        const index = new HashIndex();
        await index.insert(docs, index_name);
        return index;
    }

    getEntries(token: string, indexName: string, max_results?: number): Promise<{ idf_entry: InverseDocumentFrequencyEntry; tf_entries: TermFrequencyEntry[]; }> {
        const pk = `${indexName}#${token}`;
        const idf = this.index[pk].get(stringify(UUID_000)) as InverseDocumentFrequencyEntry;


        const tf_array = Array
                .from(this.index[pk].values())
                .slice(0,max_results)
                .filter((entry) => entry.id !== UUID_000) as TermFrequencyEntry[];


        return Promise.resolve({ idf_entry: idf, tf_entries: tf_array });
    }

    getEntriesGlobal(token: string, indexName: string, max_results?: number): Promise<{ idf_entry: InverseDocumentFrequencyEntry; tf_entries: TermFrequencyEntry[]; }> {
        const pk = `${indexName}#${token}`;
        const idf = this.index[pk].get(stringify(UUID_000)) as InverseDocumentFrequencyEntry;

        //collect the first max_results entries
        const tf_array : TermFrequencyEntry[] = [];
        for (let pair of this.tf_global_index[pk].entriesReversed()) {
            const entry = pair[1];
            tf_array.push(entry);

            if (max_results && tf_array.length >= max_results) {
                break;
            }
        }

        return Promise.resolve({ idf_entry: idf, tf_entries: tf_array });
    }

    update_global_entry_internal(global_stats: GlobalStatisticsEntry): Promise<void> {
        this.global_entry_store[global_stats.pk] = global_stats;
        return Promise.resolve()
    }

    get_global_entry_internal(indexName: string): Promise<GlobalStatisticsEntry> {
        const pk = `${indexName}#global_stats`;
        const entry = this.global_entry_store[pk];
        return Promise.resolve(entry);
    }

    insert_internal(tf_entries: TermFrequencyEntry[], idf_entries: InverseDocumentFrequencyEntry[]): Promise<void> {
        for (const entry of tf_entries) {
            this.insert_into_index(entry);
            this.insert_into_global_tf_index(entry);
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

    insert_into_global_tf_index(entry: TermFrequencyEntry)
    {
        //if the pk doesn't exist, create it
        if (!this.tf_global_index[entry.pk]) {
            this.tf_global_index[entry.pk] = new BTree<number, TermFrequencyEntry>(
                undefined,
            );
        }

        //intperpret tf binary is uint32
        //get the first 4 bytes of the tf
        this.tf_global_index[entry.pk].set(entry.tf, entry);
    }

    delete_from_index(entry: TermFrequencyEntry | InverseDocumentFrequencyEntry): void {
        const uuid = stringify(entry.id);
        this.index[entry.pk].delete(uuid);
    }

}