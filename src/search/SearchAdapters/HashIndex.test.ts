import { HashIndex } from "./HashIndex";
import test_docs from "../../test_data/test_docs.json";
import { IngestionDocument } from "../../documents/FeatherDocumentStore.d";

describe('HashIndex', () => {
    
    test('insert', async () => {
        const docs = test_docs as IngestionDocument[];

        const index = await HashIndex.from(docs, "test_index");
    }, 100000);

    test('query', async () => {
        const docs = test_docs as IngestionDocument[];
        const index = await HashIndex.from(docs, "test_index");

        const scores = await index.query("franchise tax");

        const top_score = scores[0];

        console.log("Top Score: ", top_score);

        const top_doc = docs.find(doc => doc.uuidv7 === top_score.id);
        
        console.log("Top Document: ", top_doc);
    }, 10000);

    test('delete', async () => {
        const docs = test_docs as IngestionDocument[];
        const index = await HashIndex.from(docs, "test_index");

        const scores = await index.query("franchise tax");

        const scored_docs = scores.map(score => docs.find(doc => doc.uuidv7 === score.id));

        await index.delete(scored_docs as IngestionDocument[]);

        const new_scores = await index.query("franchise tax");

        expect(new_scores.length).toBe(0);
        
    }, 10000);
});