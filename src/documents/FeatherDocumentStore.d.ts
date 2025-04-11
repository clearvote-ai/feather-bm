

//the interface type for documents you want to ingest
export interface IngestionDocument 
{
    uuidv7: string, //must be a uuidv7
    title?: string,
    text: string,
    published?: boolean,
    //TODO: ingest these fields in the document store
    [key: string]: any //allow any other properties
}

//NOTE: This is the type thats returned from FeatherDocumentStore after hydration (aka. decompression)
export type FeatherDocument = {
    pk: string, //partition key
    id: string, //sort key UUIDv7
    sha: string, //sha256 hash
    title?: string,
    text: string, 
    published?: boolean,
}

//NOTE: This is the type that is actually stored in the data store
//Entry has shorter field names to save space
export type FeatherDocumentEntry = {
    pk: string, //partition key
    id: Uint8Array, //sort key UUIDv7
    sha: Uint8Array, //sha256 hash
    t?: string, //title
    txt: Uint8Array, //brotli compressed text as binary
    p?: boolean, //is this document published and indexed - availible for search
}