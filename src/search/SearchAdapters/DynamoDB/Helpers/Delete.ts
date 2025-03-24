import { BatchWriteCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { InverseDocumentFrequencyEntry, TermFrequencyEntry } from "../../../FeatherBMIndex.d";
import { DYNAMO_DB_MAX_BATCH_SIZE } from "../DynamoDBIndex";

export namespace DynamoDBDelete {
   export async function deleteDynamoDBEntryBatch(client: DynamoDBDocumentClient, table_name: string, requests: (InverseDocumentFrequencyEntry | TermFrequencyEntry)[]): Promise<void> {
      for (let i = 0; i < requests.length; i += DYNAMO_DB_MAX_BATCH_SIZE) 
      {
         const batch = requests.slice(i, i + DYNAMO_DB_MAX_BATCH_SIZE);
         const deleteRequests = batch.map(entry => ({
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
   }
}