import boto3
import pandas as pd
import csv
import s3fs
import sys
from datetime import datetime


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
def lastUploaded(BucketName, FolderName):
    s3resource = boto3.resource('s3')
    inpBucket = s3resource.Bucket(BucketName)
    latestModifiedDate = datetime(1984, 1, 1, 0)
    latestModifedReferenceFile = ""
    for obj in inpBucket.objects.all():
        Condition1 = (obj.key.find(FolderName) != -1)
        Condition2 = (datetime.strptime(str(obj.last_modified), '%Y-%m-%d %H:%M:%S+00:00') > latestModifiedDate)
        if(Condition1 and Condition2):
            latestModifiedDate = datetime.strptime(str(obj.last_modified), '%Y-%m-%d %H:%M:%S+00:00')
            latestModifedReferenceFile = str(obj.key)
    return latestModifedReferenceFile


#Prints details of Objects uploaded
inputBucket = sys.argv[1]
inputObjectKey = sys.argv[2]


print("Bucket Name:",inputBucket)
print("Key Value:",inputObjectKey)


#Reference File location
referenceFolder = getSecondFolder(inputObjectKey)
print("Taking latest file from", referenceFolder, "as reference.")


latestReferenceFile = lastUploaded(inputBucket, referenceFolder)
print("Reference File Key: ", latestReferenceFile)

pathInput1 = 's3://' + inputBucket + '/' + inputObjectKey
pathInput2 = 's3://' + inputBucket +'/' + latestReferenceFile

currDate = datetime.now();
print(currDate.strftime('%d/%m/%Y'))
pathOutput = 's3://kesharia/yusjain/output' + currDate.strftime('%m%d%Y%H%M%S') + '.csv'


#Read File1
dfo = pd.read_csv(pathInput1, delimiter = '\t')

#Read File2
dfi2 = pd.read_csv(pathInput2, delimiter = '\t')

#Output DataFrame
dfo['output2'] = dfo['output1']

for ind1 in dfo.index:
    for ind2 in dfi2.index:
        Condition1 = (dfo['input1'][ind1]==dfi2['input1'][ind2])
        Condition2 = (dfo['input2'][ind1] == dfi2['input2'][ind2])
        if(Condition1 and Condition2):
            dfo.loc[ind1, 'output2'] = dfi2.loc[ind2, 'output1'].copy()
            break
        else:
            pass

#Write output in modified file
dfo.to_csv(pathOutput, index = False, sep='\t')


#Print contents of files
print(dfo.head())


#DDB update of file written in Output Bucket

session = boto3.session.Session(region_name= 'us-west-2')
DDBResource = session.resource('dynamodb')
S3Client = boto3.client('s3')

#DDB Table Declration
table = DDBResource.Table('yusjainJobRecords')

outputBucketName = 'kesharia'
outputFolderName = 'yusjain'
outputFileKey = lastUploaded(outputBucketName, outputFolderName)
outputObj = S3Client.get_object(
    Bucket=outputBucketName,
    Key=outputFileKey
)

table.put_item(
    Item={
        'Object Key': outputFileKey,
        'Bucket Name': outputBucketName,
    }
)
print("DDB Table Update Successful")



