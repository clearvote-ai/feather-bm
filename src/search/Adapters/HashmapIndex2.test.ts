import { IndexedDocument } from "../BM25/InvertedIndex";
import { HashMapIndex } from "./HashmapIndex";
import test_docs from "../test_data/arkansas_2023.json";



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

    test('delete', async () => {
        const index = HashMapIndex.from(test_docs as IndexedDocument[]);

        const scores = await index.query("franchise tax");

        const full_docs = test_docs as IndexedDocument[];

        const scored_docs = scores.map(score => full_docs.find(doc => doc.sortkey === score.id));

        await index.delete_batch(scored_docs as IndexedDocument[]);

        const new_scores = await index.query("franchise tax");

        expect(new_scores.length).toBe(0);

        
    }, 100000);
});