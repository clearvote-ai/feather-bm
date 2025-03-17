import { IndexedDocument } from "../BM25/InvertedIndex";
import { HashMapIndex } from "./HashmapIndex";
import test_docs from "../BM25/test_data/arkansas_2023.json";



describe('HashmapIndex', () => {
    test('insert', async () => {
        const docs = test_docs as IndexedDocument[];

        const index = HashMapIndex.from(test_docs as IndexedDocument[]);

        await index.insert_batch(docs);
    }, 100000);


    test('query', async () => {
        const index = HashMapIndex.from(test_docs as IndexedDocument[]);

        const scores = await index.query("franchise tax");

        const top_score = scores[0];

        const docs = test_docs as IndexedDocument[];

        const top_doc = docs.find(doc => doc.sortkey === top_score.id);

        console.log("Top Score: ", top_score);
        console.log("Top Document: ", top_doc);

    }, 100000);
});