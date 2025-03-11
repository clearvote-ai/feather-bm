import test_docs from "../test_data/arkansas_2023.json";
import { IndexedDocument } from "./BM25";
import { buildInvertedIndex } from "./InvertedIndex";
import { computeBM25ScoresConcurrent } from "./Search";

//$0.125 per million read request units
const DYNAMODB_READ_REQUEST_PER_1M = 0.125;
const DYNAMODB_READ_REQUEST_PRICE = DYNAMODB_READ_REQUEST_PER_1M/1_000_000;

//$0.625 per million write request units
const DYNAMODB_WRITE_REQUEST_PER_1M = 0.625;
const DYNAMODB_WRITE_REQUEST_PRICE = DYNAMODB_WRITE_REQUEST_PER_1M/1_000_000;

//$0.0004 per 1k S3 read requests
const S3_READ_REQUEST_PER_1K = 0.0004;
const S3_READ_REQUEST_PRICE = S3_READ_REQUEST_PER_1K/1_000;

describe('Search', () => {
    test('build index', async () => {

        const docs = test_docs as IndexedDocument[];

        const index = await buildInvertedIndex(docs);

        const keys = Object.keys(index.invertedIndex);
        //count the number of words in the index
        const word_count = keys.length;
        
        console.log("Word count: ", word_count);

        const average_documents_per_word = keys.reduce((acc, key) => acc + Object.keys(index.invertedIndex[key].documents).length, 0) / word_count;
        const max_documents_per_word = keys.reduce((acc, key) => Math.max(acc, Object.keys(index.invertedIndex[key].documents).length), 0);
        const min_documents_per_word = keys.reduce((acc, key) => Math.min(acc, Object.keys(index.invertedIndex[key].documents).length), 1000000);

        console.log("Average documents per word: ", average_documents_per_word);
        console.log("Max documents per word: ", max_documents_per_word);
        console.log("Min documents per word: ", min_documents_per_word);

        expect(true).toBe(true);
    });

    
    test('query', async () => {
        const docs = test_docs as IndexedDocument[];

        const index = await buildInvertedIndex(docs);

        const query = "franchise tax";

        const {
            stats,
            scores
        } = await computeBM25ScoresConcurrent(query, index);

        console.log("Query Stats: ", stats);
        console.log("Scores: ", scores);
        
    });

});