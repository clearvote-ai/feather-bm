import test_feather_docs from "../../test_data/test_feather_docs.json";
import { FeatherDocument, IngestionDocument } from "../../documents/FeatherDocumentStore.d";
import { HashDocumentStore } from "./HashDocumentStore";
import { parse, stringify } from "uuid";

describe('HashDocumentStore', () => {
    test('insert', async () => {
        const docs = test_feather_docs as unknown as FeatherDocument[];

        const first_doc_raw = docs[0];
        const first_doc_uuid = first_doc_raw.id;
        const first_doc_uuid_bytes = parse(first_doc_uuid);
        const stringified_uuid = stringify(first_doc_uuid_bytes);

        expect(stringified_uuid).toBe(first_doc_raw.id);

        const store = await HashDocumentStore.from([first_doc_raw], "test_index", false);

        //"01857a13-dc00-7b19-86a7-ba83ceee585e"
        const first_doc = await store.get("01857a13-dc00-7b19-86a7-ba83ceee585e", "test_index");

        expect(first_doc).toBeDefined();
        expect(first_doc?.id).toEqual(stringified_uuid);
        expect(first_doc?.title).toEqual(first_doc_raw.title);
        expect(first_doc?.text).toEqual(first_doc_raw.text);
        expect(first_doc?.published).toEqual(first_doc_raw.published);
        expect(first_doc?.pk).toEqual("test_index");

        console.log(first_doc);

    }, 10000);

    test('insert_compressed', async () => {
        const docs = test_feather_docs as unknown as FeatherDocument[];

        const first_doc_raw = docs[0];
        const first_doc_uuid = first_doc_raw.id;
        const first_doc_uuid_bytes = parse(first_doc_uuid);
        const stringified_uuid = stringify(first_doc_uuid_bytes);

        expect(stringified_uuid).toBe(first_doc_raw.id);

        //TODO: disable text compression for testing
        const store = await HashDocumentStore.from([first_doc_raw], "test_index", true);

        //"01857a13-dc00-7b19-86a7-ba83ceee585e"
        const first_doc = await store.get("01857a13-dc00-7b19-86a7-ba83ceee585e", "test_index");

        expect(first_doc).toBeDefined();
        expect(first_doc?.id).toEqual(stringified_uuid);
        expect(first_doc?.title).toEqual(first_doc_raw.title);
        expect(first_doc?.text).toEqual(first_doc_raw.text);
        expect(first_doc?.published).toEqual(first_doc_raw.published);
        expect(first_doc?.pk).toEqual("test_index");

        console.log(first_doc);

    }, 10000);

    test('large_compression_test', async () => {
        const docs = test_feather_docs as unknown as FeatherDocument[];
        const first_doc_raw = docs[0];
        const first_doc_uuid = first_doc_raw.id;
        const first_doc_uuid_bytes = parse(first_doc_uuid);
        const stringified_uuid = stringify(first_doc_uuid_bytes);

        expect(stringified_uuid).toBe(first_doc_raw.id);

        //TODO: disable text compression for testing
        const store = await HashDocumentStore.from(docs.slice(0,100), "test_index", true);

        //"01857a13-dc00-7b19-86a7-ba83ceee585e"
        const first_doc = await store.get("01857a13-dc00-7b19-86a7-ba83ceee585e", "test_index");

        expect(first_doc).toBeDefined();
        expect(first_doc?.id).toEqual(stringified_uuid);
        expect(first_doc?.title).toEqual(first_doc_raw.title);
        expect(first_doc?.text).toEqual(first_doc_raw.text);
        expect(first_doc?.published).toEqual(first_doc_raw.published);
        expect(first_doc?.pk).toEqual("test_index");

        console.log(first_doc);
    }, 10000);

    test('search_by_title', async () => {
        const docs = test_feather_docs as unknown as FeatherDocument[];

        const store = await HashDocumentStore.from(docs, "test_index");

        const search_title = "House Bill";
        const results = await store.search_by_title(search_title);

        expect(results.length).toBeGreaterThan(0);
        results.forEach(result => {
            expect(result.t?.toLowerCase()).toContain(search_title.toLowerCase());
        });
    }, 10000);

    test('delete', async () => {
        const docs = test_feather_docs as unknown as FeatherDocument[];

        const store = await HashDocumentStore.from(docs, "test_index");

        const first_doc_raw = docs[0];
        const first_doc_uuid = first_doc_raw.id;
        const first_doc_uuid_bytes = parse(first_doc_uuid);

        const first_doc = await store.get("01857a13-dc00-7b19-86a7-ba83ceee585e", "test_index");
        expect(first_doc).toBeDefined();

        await store.delete([first_doc_uuid], "test_index");

        const deleted_doc = await store.get("01857a13-dc00-7b19-86a7-ba83ceee585e", "test_index");
        expect(deleted_doc).toBeUndefined();
    }, 10000);

    test('document_exists', async () => {
        const docs = test_feather_docs as unknown as FeatherDocument[];

        const store = await HashDocumentStore.from(docs, "test_index");

        const first_doc_raw = docs[0];
        const first_doc_uuid = first_doc_raw.id;
        const first_doc_uuid_bytes = parse(first_doc_uuid);

        const exists = await store.document_exists([first_doc_raw], "test_index");
        expect(exists.length).toBe(1);
        expect(exists[0]).toBeDefined();
        expect(exists[0]?.id).toEqual(first_doc_uuid_bytes);
    }, 10000);

    test('get', async () => {
        const docs = test_feather_docs as unknown as FeatherDocument[];

        const store = await HashDocumentStore.from(docs, "test_index");

        const first_doc_raw = docs[0];
        const first_doc_uuid = first_doc_raw.id;
        const first_doc_uuid_bytes = parse(first_doc_uuid);

        const first_doc = await store.get("01857a13-dc00-7b19-86a7-ba83ceee585e", "test_index");
        expect(first_doc).toBeDefined();
        expect(first_doc?.id).toEqual("01857a13-dc00-7b19-86a7-ba83ceee585e");
        expect(first_doc?.title).toEqual(first_doc_raw.title);
        expect(first_doc?.text).toEqual(first_doc_raw.text);
        expect(first_doc?.published).toEqual(first_doc_raw.published);
    }, 10000);

}); // end of describe block