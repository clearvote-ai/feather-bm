import { FeatherDocumentEntry, IngestionDocument } from "../FeatherDocumentStore.d";
import { FeatherDocumentStore, SHAToHexString } from "../FeatherDocumentStore";
import BTree from "sorted-btree";
import { stringify } from "uuid";

/*
Scan a range of items: t.forRange(lowKey, highKey, includeHiFlag, (k,v) => {...})
Count the number of keys in a range: c = t.forRange(loK, hiK, includeHi, undefined)
Get pairs for a range of keys ([K,V][]): t.getRange(loK, hiK, includeHi)
Get next larger key/pair than k: t.nextHigherKey(k), t.nextHigherPair(k)
Get largest key/pair that is lower than k: t.nextLowerKey(k), t.nextLowerPair(k)
*/


export class HashDocumentStore extends FeatherDocumentStore 
{

    //TODO: remove pk from this implementation its unecessary for testing
    store: { [pk: string]: BTree<string, FeatherDocumentEntry> } = {};
    //secondary index for title maps from title to document id
    title_index: { [pk: string]: BTree<string, string> } = {};
    //secondary index for sha maps from sha to document id
    sha_index: { [pk: string]: BTree<string, string> } = {};

    public static async from(documents: IngestionDocument[], indexName: string): Promise<HashDocumentStore> {
        const store = new HashDocumentStore(indexName, false);
        await store.insert(documents);
        return store;
    }

    get_document_by_sha(shas: ArrayBuffer[]): Promise<(FeatherDocumentEntry | undefined)[]> {
        const results: (FeatherDocumentEntry | undefined)[] = [];
        for (const sha of shas) {
            const sha_string = SHAToHexString(new Uint8Array(sha));
            const entry = this.sha_index[this.indexName].get(sha_string);
            if(entry) {
                const document = this.store[this.indexName].get(entry);
                results.push(document);
            }
        }
        return Promise.resolve(results);
    }

    get_document_by_uuid(uuid: Uint8Array): Promise<FeatherDocumentEntry | undefined> {
        const uuid_string = stringify(uuid);
        const entry = this.store[this.indexName].get(uuid_string);
        return Promise.resolve(entry);
    }

    search_by_title(title: string): Promise<FeatherDocumentEntry[]> {
        //search for titles that begin with or equal the given title
        //eg. title:"fran" should match "franchise tax"
        const results: FeatherDocumentEntry[] = [];
        const title_index = this.title_index[this.indexName];
        const title_key = title.toLowerCase();


        //get the next highest key while the key still begins with the title
        for (let p of this.title_index[this.indexName].entries(title_key)) {
            //if the key does not begin with the title then break
            if(!p[0].startsWith(title_key)) break;

            const entry = this.store[this.indexName].get(p[1]);
            if(entry) {
                results.push(entry);
            }
            
        }

        return Promise.resolve(results);

    }

    insert_internal(documents: FeatherDocumentEntry[]): Promise<Uint8Array[]> {
        const uuids: Uint8Array[] = [];
        for (const entry of documents) {
            this.insert_into_store(entry);
            uuids.push(entry.id);
        }
        return Promise.resolve(uuids);
    }

    delete_internal(uuids: Uint8Array[]): Promise<Uint8Array[]> {
        const deleted: Uint8Array[] = [];
        for (const uuid of uuids) {
            this.delete_from_store(uuid);
            deleted.push(uuid);
        }
        return Promise.resolve(deleted);
    }

    insert_into_store(entry: FeatherDocumentEntry): void {
        const uuid = stringify(entry.id);
        if(!this.store[this.indexName]) {
            this.store[this.indexName] = new BTree<string, FeatherDocumentEntry>();
        }

        if(!this.title_index[this.indexName]) {
            this.title_index[this.indexName] = new BTree<string, string>();
        }

        if(!this.sha_index[this.indexName]) {
            this.sha_index[this.indexName] = new BTree<string, string>();
        }

        this.store[this.indexName].set(uuid, entry);
        if(entry.t) this.title_index[this.indexName].set(entry.t.toLowerCase(), uuid);
        const sha_string = SHAToHexString(entry.sha);
        this.sha_index[this.indexName].set(sha_string, uuid);
    }

    delete_from_store(id: Uint8Array): void {
        const uuid = stringify(id);
        if(this.store[this.indexName]) {
            this.store[this.indexName].delete(uuid);
        }
    }
}