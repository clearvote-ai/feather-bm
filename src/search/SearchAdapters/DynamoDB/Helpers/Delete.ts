import { BatchWriteCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { DynamoDBIDFEntry, DynamoDBIndexEntry } from "../DynamoDBIndex";

export async function deleteDynamoDBEntryBatch(client: DynamoDBDocumentClient, table_name: string, entries: DynamoDBIndexEntry[]): Promise<void> {
   const deleteRequests = entries.map(entry => ({
      DeleteRequest: {
         Key: {
            pk: entry.pk,
            id: entry.id
         }
      }
   }));
   // DynamoDB allows a maximum of 25 items per batch write
   if (deleteRequests.length > 25) {
      throw new Error(`Batch size exceeds maximum of 25 items. Current size: ${deleteRequests.length}`);
   }
   // Create the params for the BatchWriteCommand
   const params = {
      RequestItems: {
         [table_name]: deleteRequests
      }
   };

   try {
      await client.send(new BatchWriteCommand(params));
   }
   catch (error) {
      console.error("Error deleting entries from DynamoDB", { error });
      throw error;
   }
}

export async function deleteDynamoDBIDFBatch(client: DynamoDBDocumentClient, table_name: string, entries: DynamoDBIDFEntry[]): Promise<void> {
   const deleteRequests = entries.map(entry => ({
      DeleteRequest: {
         Key: {
            pk: entry.pk,
            id: entry.id
         }
      }
   }));
   // DynamoDB allows a maximum of 25 items per batch write
   if (deleteRequests.length > 25) {
      throw new Error(`Batch size exceeds maximum of 25 items. Current size: ${deleteRequests.length}`);
   }
   // Create the params for the BatchWriteCommand
   const params = {
      RequestItems: {
         [table_name]: deleteRequests
      }
   };

   try {
      await client.send(new BatchWriteCommand(params));
   }
   catch (error) {
      console.error("Error deleting entries from DynamoDB", { error });
      throw error;
   }
}