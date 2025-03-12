import { BM25Score, buildInvertedIndex, FeatherBMIndex, IndexedDocument, InvertedIndex, InvertedIndexEntry, QueryStats } from "../BM25/InvertedIndex";


export class MockDynamoDB extends FeatherBMIndex
{
    invertedIndex: InvertedIndex = {};
    document_token_counts: { [ doc_id: string ] : number } = {};
    averageDocumentLength: number = 0;
    documentCount: number = 0;

    public static async from_test_documents(test_documents: IndexedDocument[]): Promise<MockDynamoDB> {
        const {
            invertedIndex, 
            document_token_counts, 
            averageDocumentLength, 
            documentCount
        } = buildInvertedIndex(test_documents);

        const mockDynamoDB = new MockDynamoDB();
        mockDynamoDB.invertedIndex = invertedIndex;
        mockDynamoDB.document_token_counts = document_token_counts;
        mockDynamoDB.averageDocumentLength = averageDocumentLength;
        mockDynamoDB.documentCount = documentCount;

        return mockDynamoDB;
    }

    async getInvertedIndexEntry(token: string): Promise<InvertedIndexEntry | undefined> {
        return this.invertedIndex[token];
    }

    async getAverageDocumentLength(): Promise<number> {
        return this.averageDocumentLength;
    }
    
}