import boto3

DDBResource = boto3.resource('dynamodb')
S3Resource = boto3.resource('s3')

table = DDBResource.Table('yusjainJobRecords')

bucketName = 'kesharia'

bucket = S3Resource.Bucket(bucketName)
latestModifiedDate = datetime(1984, 1, 1, 0)
latestModifedFile = ""
for obj in bucket.objects.all():
    Condition1 = (obj.key.find(FolderName) != -1)
    Condition2 = (datetime.strptime(str(obj.last_modified), '%Y-%m-%d %H:%M:%S+00:00') > latestModifiedDate)
    if(Condition1 and Condition2):
        latestModifiedDate = datetime.strptime(str(obj.last_modified), '%Y-%m-%d %H:%M:%S+00:00')
        latestModifedFile = str(obj.key)


table.put_item(
    Item={
        'Object Key': latestModifedFile
        'Bucket Name': bucketName
        'Upload Date': latestModifiedDate
    }
)
print("Table Update Successful")


