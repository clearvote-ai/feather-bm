import { uuidv7 } from "uuidv7";
import { IngestionDocument } from "../documents/FeatherDocumentStore.d";
import raw_docs from "./arkansas_2023.json"
import { parse, stringify } from "uuid";


interface GovernmentDocumentMetadataEntry {
    sortkey: string; //"METADATA#" + uuid
    short_filename: string;
    fully_qualified_filename: string;
    title?: string;
    subtitle?: string;
    summary?: string;
    tags?: string[];
    //outline?: Outline;
    //clear_legislation_score_metadata?: ClearLegislationScoreMetadata;
    current_page_count: number;
    full_creator_name: string;
    creator_id?: string;
    document_id: string;
    created_at: string;
    updated_at: string;
    current_version: number;
    //legislation_type: LegislationType;
    //legislation_status: LegislationStatus;
    LIKECount?: number;
    DISLIKECount?: number;
    commentCount?: number;
    full_text: string;
}

const docs = raw_docs as GovernmentDocumentMetadataEntry[];


export function buildTestDocs() : IngestionDocument[]
{
    return docs.map((doc) => {
        // Parse the created_at timestamp and convert it to epoch milliseconds
        const createdAt = new Date(doc.created_at).getTime();

        // Generate a uuidv7
        const uuid = uuidv7();

        // Convert the timestamp to a byte array (big-endian)
        const timestampBytes = new Uint8Array(8);
        const view = new DataView(timestampBytes.buffer);
        view.setBigInt64(0, BigInt(createdAt), false); // false for big-endian

        // Convert the uuid to a byte array
        const uuidBytes = parse(uuid);

        // Overwrite the first 6 bytes of the uuid with the last 6 bytes of the timestamp
        for (let i = 0; i < 6; i++) {
            uuidBytes[i] = timestampBytes[2 + i]; // Start from the 3rd byte to get the last 6 bytes
        }

        // Convert the modified byte array back to a UUID string
        const modifiedUuid = stringify(uuidBytes);

        return {
            uuidv7: modifiedUuid,
            title: doc.title || "",
            text: doc.full_text || "",
        } satisfies IngestionDocument;
    });
}

