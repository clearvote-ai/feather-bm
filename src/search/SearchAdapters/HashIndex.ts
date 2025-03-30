import { InverseDocumentFrequencyEntry, TermFrequencyEntry, GlobalStatisticsEntry } from "../FeatherBMIndex.d";
import { FeatherBMIndex, UUID_000 } from "../FeatherBMIndex";
import BTree from "sorted-btree";
import { stringify } from "uuid";
import { IngestionDocument } from "../../documents/FeatherDocumentStore.d";

export class HashIndex extends FeatherBMIndex
{

    index: { [pk: string]: BTree<string, TermFrequencyEntry | InverseDocumentFrequencyEntry> } = {
        [this.indexName] : new BTree<string, TermFrequencyEntry | InverseDocumentFrequencyEntry>()
    }
    tf_global_index : { [pk: string]: BTree<number, TermFrequencyEntry> } = {
        [this.indexName] : new BTree<number, TermFrequencyEntry>()
    }

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

    getEntries(token: string, max_results?: number): Promise<{ idf_entry: InverseDocumentFrequencyEntry; tf_entries: TermFrequencyEntry[]; }> {
        const pk = `${this.indexName}#${token}`;
        const idf = this.index[pk].get(stringify(UUID_000)) as InverseDocumentFrequencyEntry;


        const tf_array = Array
                .from(this.index[pk].values())
                .slice(0,max_results)
                .filter((entry) => entry.id !== UUID_000) as TermFrequencyEntry[];


        return Promise.resolve({ idf_entry: idf, tf_entries: tf_array });
    }

    getEntriesGlobal(token: string, max_results?: number): Promise<{ idf_entry: InverseDocumentFrequencyEntry; tf_entries: TermFrequencyEntry[]; }> {
        const pk = `${this.indexName}#${token}`;
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
        this.global_entry = global_stats;
        return Promise.resolve()
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
        const tf_bytes = entry.tf.slice(0, 4);
        const tf_number = Uint8ArrayToUint32(tf_bytes);
        const tf = Number(tf_number);
        this.tf_global_index[entry.pk].set(tf, entry);
    }

    delete_from_index(entry: TermFrequencyEntry | InverseDocumentFrequencyEntry): void {
        const uuid = stringify(entry.id);
        this.index[entry.pk].delete(uuid);
    }

}

function Uint8ArrayToUint32(array: Uint8Array): number {
    if (array.length !== 4) {
        //pad the array to 4 bytes
        const paddedArray = new Uint8Array(4);
        paddedArray.set(array);
        array = paddedArray;
    }
    const dataView = new DataView(array.buffer, array.byteOffset, array.byteLength);
    return dataView.getUint32(0, true); // true for little-endian, false for big-endian
}

function uint8ArrayToUint64(array: Uint8Array): bigint {
    if (array.length !== 8) {
        //pad the array to 8 bytes
        const paddedArray = new Uint8Array(8);
        paddedArray.set(array);
        array = paddedArray;
    }
  
    const dataView = new DataView(array.buffer, array.byteOffset, array.byteLength);
    return dataView.getBigUint64(0, true); // true for little-endian, false for big-endian
}