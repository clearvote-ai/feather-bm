import { IndexedDocument } from "../../../FeatherTypes";
import { HashMapIndex } from "./HashmapIndex";
import { getTestDocs } from "../../test_data/TestData";


describe('HashmapIndex', () => {
    test('insert', async () => {
        const docs = getTestDocs();

        const index = new HashMapIndex([docs[0]], "test_index", "FeatherIndex");

        await index.insert(docs);
    }, 100000);


    test('query', async () => {
        const docs = getTestDocs();
        const index = new HashMapIndex(docs, "test_index", "FeatherIndex");

        const scores = await index.query("franchise tax");

        const top_score = scores[0];

        const top_doc = docs.find(doc => doc.uuidv7 === top_score.id);

        console.log("Top Score: ", top_score);
        console.log("Top Document: ", top_doc);

    }, 100000);

    test('delete', async () => {
        const docs = getTestDocs();
        const index = new HashMapIndex(docs, "test_index", "FeatherIndex");

        const scores = await index.query("franchise tax");

        const scored_docs = scores.map(score => docs.find(doc => doc.uuidv7 === score.id));

        await index.delete(scored_docs as IndexedDocument[]);

        const new_scores = await index.query("franchise tax");

        expect(new_scores.length).toBe(0);

        
    }, 100000);
});