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

export interface GovIngestionDocument extends IngestionDocument 
{
    tags?: string[];
    summary?: string;
}


export function buildTestDocs() : GovIngestionDocument[]
{
    return docs.map((doc) => {
        return {
            text: doc.full_text,
            iso8601: doc.created_at,
            published: true,
            title: doc.title,
            summary: doc.summary,
            tags: doc.tags
        } as GovIngestionDocument;
    });
}

