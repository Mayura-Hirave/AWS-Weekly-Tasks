import boto3
#import botocore
import random
from aws_access_key import aws_access_key_ID,aws_secret_access_KEY


class CustomError(Exception):
    def __init__(self, description):
        self.description=description
    def __str__(self):  # overriding print()
        return self.description
        
class Utility:
    @staticmethod
    def compareMetadata(dict1, dict2):
        for key,val in dict1.items():
            if key not in dict2 or dict2[key]!=val:
                return False
        return True

    # list1 = [{'Key': 'tag1', 'Value': 'tag1Val1'}, {'Key': 'tag2', 'Value': 'tag2Val1'}]
    @staticmethod
    def compareTags( list1, list2):
        for dict1 in list1:
            if dict1 not in list2:
                return False
        return True

    @staticmethod
    def convertDictToString(tags):
        return "&".join([k + "=" + v for k, v in tags.items()])

    @staticmethod
    def takeDictInput(itemsCount):
        dict1=dict()
        for i in range(itemsCount):
            key_val = input("Enter in format => Key:Value  -> ").split(":")
            dict1[key_val[0]] = key_val[1]
        return dict1

    @staticmethod
    def convertDictToTagset(dict1):
        list1=[]
        for key,val in dict1.items():
            dict2=dict()
            dict2['Key']=key
            dict2['Value']=val
            list1.append(dict2)
        return list1

class s3bucket:
    session = boto3.Session(aws_access_key_id=aws_access_key_ID, aws_secret_access_key=aws_secret_access_KEY,
                            region_name="us-west-1")
    s3 = session.resource("s3")
    s3Client = session.client("s3")  # created only for accessing object's tags - get_object_tagging()
    def __init__(self,bucketName):
        try:
            self.bucket = s3bucket.s3.Bucket(bucketName)
            if bucketName not in [bucket.name for bucket in s3bucket.s3.buckets.all()]: # checking if bucket already exists
                self.bucket.create(CreateBucketConfiguration={'LocationConstraint': 'us-west-1' })
        except Exception as e:
            raise CustomError("Error in s3bucket.__init__(): "+str(e))
    def showObjects(self):
        cntOfObjs=0
        try:
            print("Object Key \t Metadata \t Tags")
            for objectSummary in self.bucket.objects.all():
                object = self.bucket.Object(objectSummary.key)
                objTags = s3bucket.s3Client.get_object_tagging(Bucket=self.bucket.name, Key=object.key)['TagSet']
                print(object.key,"\t",object.metadata,"\t",objTags)
                cntOfObjs+=1
            print("Total Objects: ",cntOfObjs)
        except Exception as e:
            raise CustomError("Error in s3bucket.__init__(): "+str(e))

    def DeleteObjsBasedOnMetadataTags(self,filterMetadata, filterTags):
        objectsToDelete = []
        try:
            for objectSummary in self.bucket.objects.all():
                object=self.bucket.Object(objectSummary.key)
                objTags = s3bucket.s3Client.get_object_tagging(Bucket=self.bucket.name,Key=object.key)['TagSet']
                #print(object.metadata,objTags)
                if Utility.compareMetadata(filterMetadata, object.metadata) and Utility.compareTags(filterTags,objTags):
                    objectsToDelete.append({'Key':object.key,'VersionId':'null'})
            response=None
            #print(objectsToDelete) -deleting these matched objects
            if objectsToDelete:
                response = self.bucket.delete_objects(Delete={'Objects': objectsToDelete, 'Quiet': True})
            print("Successfully deleted",len(objectsToDelete),"objects!")
            return response
        except Exception as e:
            raise CustomError("Error in s3bucket.DeleteObjsBasedOnMetadataTags(): "+str(e))

    def uploadObj(self, objName,metadata,fileContent,tags):
        try:
            resultDict = self.bucket.put_object(Key=objName, Body=fileContent, Metadata=metadata, Tagging=tags)
            return resultDict
        except Exception as e:
            raise CustomError("Error in s3bucket.uploadObj(): " + str(e))

if __name__ == '__main__':
    try:
        while True:
            bucketName= input("Enter Bucket Name: ")
            #bucketName='my-new-s3bucket'
            myBucket = s3bucket(bucketName)
            while True:
                option=input("\nSelect any option:\n1. Put objects\n2. Display Object details\n3. Delete objects\n4. Exit\n=> ")
                if option=='1':
                    noOfObjs=input("How many objects?")
                    while not noOfObjs.isnumeric():
                        noOfObjs = input("Kindly enter integer! => ")

                    noOfObjs=int(noOfObjs)
                    for i in range(noOfObjs):
                        objName = input("Enter Key of the object: ")
                        fileContent = ""
                        path=input("Enter path of input file=> ")  #like    C:\Users\Mayura\Downloads\dummy data.txt
                        with open(path, 'rb') as fileObj:
                            fileContent=fileObj.read()
                        metadataCount = int(input("Count of metadata: "))
                        metadata = Utility.takeDictInput(metadataCount)
                        tagsCount = int(input("Number of Tags: "))
                        tags = Utility.takeDictInput(tagsCount)
                        myBucket.uploadObj(objName,metadata,fileContent,Utility.convertDictToString(tags))
                    print("Successfully uploaded",noOfObjs,"objects")
                elif option=='2':
                    myBucket.showObjects()
                elif option=='3':
                    metadataCount = int(input("Count of metadata: "))
                    metadata = Utility.takeDictInput(metadataCount)
                    tagsCount = int(input("Number of Tags: "))
                    tagsSet= Utility.convertDictToTagset(Utility.takeDictInput(tagsCount))
                    response = myBucket.DeleteObjsBasedOnMetadataTags(metadata, tagsSet)
                    if response and response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        print("Encountered Error!")
                elif option=='4':
                    break
                else:
                    print("Invalid Option!")
            option=input("\nOptions:\n1. Work on other bucket\n2. Exit\n=> ")
            if option=="1":
                continue
            elif option=="2":
                break
            else:
                print("Invalid Option!")
                break
    except CustomError as e:
        print(e)
    except Exception as e:
        print("Error: ",e)
