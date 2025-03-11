

export interface FeatherBMAdapter {
    inverted_index_get: (token: string) => Promise<{ [doc_id: string]: number } | undefined>;
    


}