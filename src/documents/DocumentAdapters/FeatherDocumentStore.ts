import { IndexedDocument } from "../../FeatherTypes";

export abstract class FeatherDocumentStore
{
    /**
     * The name of the collection in the database.
     **/
    public indexName: string;

    constructor(indexName: string)
    {
        this.indexName = indexName;
    }

    async insert(documents: IndexedDocument[] | IndexedDocument): Promise<void>
    {
        // Ensure documents is an array
        if (!Array.isArray(documents)) {
            documents = [documents];
        }
        if(documents.length === 0) throw new Error("No documents to insert");
        await this.insert_internal(documents);
    }

    async delete(documents: IndexedDocument[] | IndexedDocument): Promise<void>
    {
        // Ensure documents is an array
        if (!Array.isArray(documents)) {
            documents = [documents];
        }
        if(documents.length === 0) throw new Error("No documents to delete");
        await this.delete_internal(documents);
    }

    abstract insert_internal(documents: IndexedDocument[]) : Promise<void>;
    abstract delete_internal(documents: IndexedDocument[]) : Promise<void>;
}   