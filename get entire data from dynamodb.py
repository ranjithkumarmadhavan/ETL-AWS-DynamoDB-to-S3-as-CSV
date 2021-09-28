import json
import boto3
import logging
import sys
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Key, Attr
import decimal
import datetime
import os

# Get logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

tableName = None
fromDate = None
toDate = None
minimumRecordsPerFile = 10000

# s3 configurations
s3 = boto3.resource('s3')
bucketName = "BUCKET-NAME"
s3Folder = "demo/dynamodb-sync/data_"

def lambda_handler(event, context):
    response = None
    startTime = datetime.datetime.now()
    tableName = "TABLE_NAME"
    table = boto3.resource('dynamodb').Table(tableName)
    fileSuffix = 1
    rowCount = 0
    column_names = "tenantId,locationid_entityguid_businessdate\n"
    content = column_names
    while True:
        if not response:
            # Scan from the start.
            response = table.scan(ProjectionExpression = 'id, #c',ExpressionAttributeNames = {'#c': 'date'})
        else:
            # Scan from where you stopped previously.
            response = table.scan(ProjectionExpression = 'id, #c',ExpressionAttributeNames = {'#c': 'date'}, ExclusiveStartKey=response['LastEvaluatedKey'])
        
        for item in response["Items"]:
            rowCount += 1
            content += f"{item['id']},{item['date']}\n"
            # break
            
        if rowCount >= minimumRecordsPerFile:
            out_file = s3Folder + f"{fileSuffix}.csv"
            s3.Object(bucketName, out_file).put(Body = content)
            del content
            del rowCount
            rowCount = 0
            content = column_names
            fileSuffix += 1
            print(response['LastEvaluatedKey'])
            print("File Completed - {0}".format(out_file))
            # break
        
        # break
    
        # Stop the loop if no additional records are
        # available.
        if 'LastEvaluatedKey' not in response:
            if content.strip() != "":
                out_file = s3Folder + f"{fileSuffix}.csv"
                s3.Object(bucketName, out_file).put(Body = content)
                fileSuffix += 1
                print("File Completed - {0}".format(out_file))
            break
    endTime = datetime.datetime.now()
    print("Time taken - {0}".format(endTime - startTime))
    
# to handle decimal fields from dynamodb
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 == 0:
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)
        

# remove the below line incase you are using this script in lambda
lambda_handler("","")