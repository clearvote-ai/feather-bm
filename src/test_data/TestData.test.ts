import { buildTestDocs } from "./TestData";
import fs from 'fs';

describe('TestData', () => {
    test('getTestDocs', () => {
        const docs = buildTestDocs();

        //save the docs to a file for debugging
        fs.writeFileSync('test_ingestion_docs.json', JSON.stringify(docs, null, 2), 'utf-8');
        expect(docs.length).toBeGreaterThan(0); // check if there are documents
    });
});