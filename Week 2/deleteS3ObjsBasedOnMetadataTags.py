import boto3
import random
from aws_access_key import aws_access_key_ID,aws_secret_access_KEY

def compareMetadata(dict1, dict2):
    for key,val in dict1.items():
        if key not in dict2 or dict2[key]!=val:
            return False
    return True

# list1 = [{'Key': 'tag1', 'Value': 'tag1Val1'}, {'Key': 'tag2', 'Value': 'tag2Val1'}]
def compareTags( list1, list2):
    for dict1 in list1:
        if dict1 not in list2:
            return False
    return True

class s3bucket:
    def __init__(self,bucketName):
        self.session = boto3.Session(aws_access_key_id=aws_access_key_ID, aws_secret_access_key=aws_secret_access_KEY,region_name="us-west-1")
        self.s3 = self.session.resource("s3")
        self.s3Client = self.session.client("s3") # created only for accessing object's tags - get_object_tagging()
        self.bucket = self.s3.Bucket(bucketName)
        if not self.isBucketExists(bucketName):
            self.bucket.create(CreateBucketConfiguration={'LocationConstraint': 'us-west-1' })

    def isBucketExists(self,bucketName):
        return bucketName in [bucket.name for bucket in self.s3.buckets.all()]

    def DeleteObjsBasedOnMetdataTags(self,filterMetadata, filterTags):
        objectsToDelete = []
        for objectSummary in self.bucket.objects.all():
            object=self.bucket.Object(objectSummary.key)
            objTags = self.s3Client.get_object_tagging(Bucket=self.bucket.name,Key=object.key)['TagSet']
            print(object.metadata,objTags)
            if compareMetadata(filterMetadata, object.metadata) and compareTags(filterTags,objTags):
                objectsToDelete.append({'Key':object.key,'VersionId':'null'})
        response=None
        print(objectsToDelete)
        if objectsToDelete:
            response = self.bucket.delete_objects(Delete={'Objects': objectsToDelete, 'Quiet': True})
        return response

    def uploadObj(self, objName,metadata,fileContent,tags):
        resultDict = self.bucket.put_object(Key=objName, Body=fileContent, Metadata=metadata, Tagging=tags)
        return resultDict

if __name__ == '__main__':
    # For creating dummy s3 objects
    metdata1 = ['meta1Val1', 'meta1Val2', 'meta1Val3']
    metdata2 = ['meta2Val1', 'meta2Val2', 'meta2Val3']
    tag1 = ['tag1Val1', 'tag1Val2', 'tag1Val3']
    tag2 = ['tag2Val1', 'tag2Val2','tag2Val3']
    fileType = ['txt', 'pdf', 'docx','csv']
    fileContent = "AWS Dummy data, AWS Dummy data, AWS Dummy data"

    myBucket=s3bucket('my-new-s3bucket')

    # adding 15 objects into `my-new-s3bucket` bucket
    for i in range(15):
        objName = "file"+str(i)+"."+fileType[random.randint(0, 3)]
        metadata = {"metadata1": metdata1[random.randint(0, 2)],"metdata2":metdata2[random.randint(0, 2)]}
        tags ='tag1'+'='+tag1[random.randint(0, 2)]+'&'+'tag2'+'='+tag2[random.randint(0, 2)]
        print(tags)
        myBucket.uploadObj(objName,metadata,fileContent,tags)

    
    # delete objects based on given metadata & tags
    #response=myBucket.DeleteObjsBasedOnMetdataTags({},[{'Key': 'tag1', 'Value': 'tag1Val2'}, {'Key': 'tag2', 'Value': 'tag2Val1'}])
    response = myBucket.DeleteObjsBasedOnMetdataTags({'metadata1':'meta1Val2'}, [])
    if not response or response['ResponseMetadata']['HTTPStatusCode']== 200:
        print("successfully deleted")
    else:
        print("Encountered Error!")
