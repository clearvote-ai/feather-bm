import { IndexedDocument } from "../BM25/InvertedIndex";
import { computeBM25ScoresConcurrent } from "../BM25/Search";
import test_docs from "../BM25/test_data/arkansas_2023.json";
import { MockDynamoDB } from "./MockDynamoDB";


describe('MockDynamoDB', () => {
    test('query', async () => {
        const docs = test_docs as IndexedDocument[];

        //TODO: flatten out the index to mock dynamoDB performance
        //TODO: make an adapter for real dynamoDB
        const mock_index = await MockDynamoDB.from_test_documents(docs);

        const query = "franchise tax";

        const result = await mock_index.query(query);

        console.log("Query Stats: ", result.stats);
        //console.log("Scores: ", result.scores.slice(0, 10));

        
    });
});