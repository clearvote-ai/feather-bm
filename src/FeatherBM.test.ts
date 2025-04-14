import { FeatherBM } from "./FeatherBM";
import test_ingestion_docs from "./test_data/test_ingestion_docs.json";
import { GovIngestionDocument } from "./test_data/TestData";


describe('FeatherBM', () => {
    test('ingestion', async () => {
        const docs = test_ingestion_docs as GovIngestionDocument[];
        const bm = await FeatherBM.fromInMemory(false);
        expect(bm).toBeInstanceOf(FeatherBM);

        await bm.insert(docs, "test_index");

        const result_1 = await bm.searchByTitle("House Bill 1070", "test_index");

        expect(result_1).toBeDefined();
        expect(result_1.length).toBe(1);

        const result_2 = await bm.query("Arkansas Citizens First Responder Safety Enhancement Fund", "test_index");

        expect(result_2).toBeDefined();
        expect(result_2[0].title).toBe("House Bill 1070 - Appropriation for Assistance to Local Law Enforcement and Emergency Medical");
        
    }, 100000);
});