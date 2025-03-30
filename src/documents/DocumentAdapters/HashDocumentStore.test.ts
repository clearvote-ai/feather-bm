import test_docs from "../../test_data/test_docs.json";
import { IngestionDocument } from "../../documents/FeatherDocumentStore.d";
import { HashDocumentStore } from "./HashDocumentStore";
import { parse, stringify } from "uuid";

describe('HashDocumentStore', () => {
    test('insert', async () => {
        const docs = test_docs as IngestionDocument[];

        const first_doc_raw = docs[0];
        const first_doc_uuid = first_doc_raw.uuidv7;
        const first_doc_uuid_bytes = parse(first_doc_uuid);
        const stringified_uuid = stringify(first_doc_uuid_bytes);

        expect(stringified_uuid).toBe(first_doc_raw.uuidv7);

        //TODO: disable text compression for testing
        const store = await HashDocumentStore.from([first_doc_raw], "test_index");

        //"01857a13-dc00-7b19-86a7-ba83ceee585e"
        const first_doc = await store.get("01857a13-dc00-7b19-86a7-ba83ceee585e");

        expect(first_doc).toBeDefined();
        expect(first_doc?.id).toEqual(first_doc_uuid_bytes);
        expect(first_doc?.title).toEqual(first_doc_raw.title);
        expect(first_doc?.text).toEqual(first_doc_raw.text);
        expect(first_doc?.published).toEqual(first_doc_raw.published);
        expect(first_doc?.pk).toEqual("test_index");

        console.log(first_doc);

    }, 100000);

    test('search_by_title', async () => {
        const docs = test_docs as IngestionDocument[];

        const store = await HashDocumentStore.from(docs, "test_index");

        const search_title = "House Bill";
        const results = await store.search_by_title(search_title);

        expect(results.length).toBeGreaterThan(0);
        results.forEach(result => {
            expect(result.t?.toLowerCase()).toContain(search_title.toLowerCase());
        });
    }, 10000);

    test('delete', async () => {
        // Test the delete functionality
    }, 10000);

    test('document_exists', async () => {
        // Test the document_exists functionality
    }, 10000);

    test('get', async () => {
        // Test the get functionality
    }, 10000);

}); // end of describe block