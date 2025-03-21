import { BatchWriteCommand, DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBIndexEntry } from "../DynamoDBIndex";
import { InvertedIndexGlobalStatistics } from "../../../BM25/InvertedIndex";
import { UpdateItemInput } from "@aws-sdk/client-dynamodb";

/*
Update ITEM API

{
   "AttributeUpdates": { 
      "string" : { 
         "Action": "string",
         "Value": { 
            "B": blob,
            "BOOL": boolean,
            "BS": [ blob ],
            "L": [ 
               "AttributeValue"
            ],
            "M": { 
               "string" : "AttributeValue"
            },
            "N": "string",
            "NS": [ "string" ],
            "NULL": boolean,
            "S": "string",
            "SS": [ "string" ]
         }
      }
   },
   "ConditionalOperator": "string",
   "ConditionExpression": "string",
   "Expected": { 
      "string" : { 
         "AttributeValueList": [ 
            { 
               "B": blob,
               "BOOL": boolean,
               "BS": [ blob ],
               "L": [ 
                  "AttributeValue"
               ],
               "M": { 
                  "string" : "AttributeValue"
               },
               "N": "string",
               "NS": [ "string" ],
               "NULL": boolean,
               "S": "string",
               "SS": [ "string" ]
            }
         ],
         "ComparisonOperator": "string",
         "Exists": boolean,
         "Value": { 
            "B": blob,
            "BOOL": boolean,
            "BS": [ blob ],
            "L": [ 
               "AttributeValue"
            ],
            "M": { 
               "string" : "AttributeValue"
            },
            "N": "string",
            "NS": [ "string" ],
            "NULL": boolean,
            "S": "string",
            "SS": [ "string" ]
         }
      }
   },
   "ExpressionAttributeNames": { 
      "string" : "string" 
   },
   "ExpressionAttributeValues": { 
      "string" : { 
         "B": blob,
         "BOOL": boolean,
         "BS": [ blob ],
         "L": [ 
            "AttributeValue"
         ],
         "M": { 
            "string" : "AttributeValue"
         },
         "N": "string",
         "NS": [ "string" ],
         "NULL": boolean,
         "S": "string",
         "SS": [ "string" ]
      }
   },
   "Key": { 
      "string" : { 
         "B": blob,
         "BOOL": boolean,
         "BS": [ blob ],
         "L": [ 
            "AttributeValue"
         ],
         "M": { 
            "string" : "AttributeValue"
         },
         "N": "string",
         "NS": [ "string" ],
         "NULL": boolean,
         "S": "string",
         "SS": [ "string" ]
      }
   },
   "ReturnConsumedCapacity": "string",
   "ReturnItemCollectionMetrics": "string",
   "ReturnValues": "string",
   "ReturnValuesOnConditionCheckFailure": "string",
   "TableName": "string",
   "UpdateExpression": "string"
}*/

/*
export interface DynamoDBIndexEntry {
    index_name: string,
    sortkey: `${DynamoDBIndexToken}`,
    documents: InverseDocumentValue[],
    idf: number
}*/

//this function returns the UpdateItemInput for deleting the document entry from "documents" attribute in the token entry
/*
export function removeDocumentsFromTokenEntry(entry: DynamoDBIndexEntry, ids: string[]): UpdateItemInput
{
    return {
        TableName: entry.index_name,
        Key: {
            index_name: entry.index_name,
            sortkey: entry.sortkey
        },
        UpdateExpression: "DELETE documents :ids",
        ExpressionAttributeValues: {
            ":ids": { SS: ids }
        }
    }

}*/