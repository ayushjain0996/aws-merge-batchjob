import boto3
import pandas as pd
import csv
import s3fs
import sys
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr

#Self Defined Functions

#Function to get reference folder name
#Used in case the Merge is called for files in the same bucket
def getSecondFolder(inputKey):
    if (inputKey.find("input1") == -1):
        return "input1"
    else:
        return "input2"

#Function to retrieve latest uploaded file in Bucket
#Function to retrieve latest uploaded file for Merge in within same bucket
def getLastUploadedFile(bucketName, folderName):
    s3resource = boto3.resource('s3')
    inputBucket = s3resource.Bucket(bucketName)
    latestModifiedDate = datetime(1984, 1, 1, 0)
    latestModifedReferenceFile = ""
    for object in inputBucket.objects.all():
        condition1 = (object.key.find(folderName) != -1)
        condition2 = (datetime.strptime(str(object.last_modified), '%Y-%m-%d %H:%M:%S+00:00') > latestModifiedDate)
        if(condition1 and condition2):
            latestModifiedDate = datetime.strptime(str(object.last_modified), '%Y-%m-%d %H:%M:%S+00:00')
            latestModifedReferenceFile = str(object.key)
    return latestModifedReferenceFile

#Function to update the status of object in DDB Table
def changePreviousObjectStatus(tableName):
    session = boto3.session.Session(region_name='us-west-2')
    ddbResource = session.resource('dynamodb')
    # DDB Table Declration
    table = ddbResource.Table(tableName)
    tableResponse = table.query(
        KeyConditionExpression=Key('FolderName').eq('yusjain/output'),
        FilterExpression=Attr('ObjectStatus').eq('Active'),
        ProjectionExpression='FileName'
    )
    previousActiveObject = tableResponse['Items']
    if previousActiveObject:
        previousActiveObjectKey = previousActiveObject[0]
        print("Changing status of the following file:")
        print(previousActiveObjectKey['FileName'])

        table.update_item(
            Key={
                'FolderName': 'yusjain/output',
                'FileName': previousActiveObjectKey['FileName']
            },
            UpdateExpression='SET ObjectStatus = :objectStatus',
            ExpressionAttributeValues={
                ':objectStatus': "Inactive"
            }
        )


#Function to add the details to the object to the DDB table
def addOdjectDetails2Table(tableName, folderName, fileName, bucketName):
    session = boto3.session.Session(region_name='us-west-2')
    ddbResource = session.resource('dynamodb')
    # DDB Table Declration
    table = ddbResource.Table(tableName)
    table.put_item(
        Item={
            'FolderName': folderName,
            'FileName': fileName,
            'BucketName': bucketName,
            'ObjectStatus': 'Active'
        }
    )

#Function to merge 2 files, one from input1 and one from input2
def mergeFunction(inputBucketName, inputObjectKey):
    # Reference File location
    referenceFolder = getSecondFolder(inputObjectKey)
    print("Taking latest file from", referenceFolder, "as reference.")

    latestReferenceFile = getLastUploadedFile(inputBucketName, referenceFolder)
    print("Reference File Key: ", latestReferenceFile)

    inputPath1 = 's3://' + inputBucketName + '/' + inputObjectKey
    inputPath2 = 's3://' + inputBucketName + '/' + latestReferenceFile

    currentDate = datetime.now();
    outputFileName = currentDate.strftime('%d%m%Y%H%M%S') + '.csv'
    outputPath = 's3://kesharia/yusjain/output/' + outputFileName

    # Read File1
    input1Dataframe = pd.read_csv(inputPath1, delimiter='\t')

    # Read File2
    input2Dataframe = pd.read_csv(inputPath2, delimiter='\t')

    # Output DataFrame
    outputDataframe = input1Dataframe
    outputDataframe['output2'] = outputDataframe['output1']

    indexListToDrop = []

    for index1 in outputDataframe.index:
        isPresent = False
        for index2 in input2Dataframe.index:
            condition1 = (outputDataframe['input1'][index1] == input2Dataframe['input1'][index2])
            condition2 = (outputDataframe['input2'][index1] == input2Dataframe['input2'][index2])
            if (condition1 and condition2):
                outputDataframe.loc[index1, 'output2'] = input2Dataframe.loc[index2, 'output1'].copy()
                isPresent = True
                break

        if (isPresent == False):
            indexListToDrop.append(index1)

    if(referenceFolder == 'input1'):
        outputDataframe = outputDataframe.drop(indexListToDrop, axis=0)

    # Write output in modified file
    outputDataframe.to_csv(outputPath, index=False, sep='\t')
    return outputFileName

#Main Functions
def batchJob():
    # Prints details of Objects uploaded
    inputBucketName = sys.argv[1]
    inputObjectKey = sys.argv[2]

    print("Bucket Name:", inputBucketName)
    print("Key Value:", inputObjectKey)

    outputFileName = mergeFunction(inputBucketName, inputObjectKey)

    # DDB update of file written in Output Bucket
    outputBucketName = 'kesharia'
    outputFolderName = 'yusjain/output'
    tableName = 'yusjainJobRecords'

    changePreviousObjectStatus(tableName)
    addOdjectDetails2Table(tableName, outputFolderName, outputFileName, outputBucketName)


batchJob()
