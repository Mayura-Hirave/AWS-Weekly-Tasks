"""
Code for copying the objects from AWS source S3 bucket to target S3 bucket as soon as objects uploaded on source S3 bucket
"""
import urllib.parse
import boto3
import json

s3 = boto3.client('s3')
def lambda_handler(event, context):
	#Get the object from the event
	srcBucketName= event['Records'][0]['s3']['bucket']['name']
	srcBucketObjKey = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
	targetBucket = 'backup-s3buckett'
	response = s3.get_object(Bucket=srcBucketName, Key=srcBucketObjKey)
	
	copy_source = {'Bucket': srcBucketName, 'Key': srcBucketObjKey}
	try:
		print ("Using waiter, to waiting for object to persist through s3 service")
		waiter = s3.get_waiter('object_exists')
		waiter.wait(Bucket=srcBucketName, Key=srcBucketObjKey)
		
		s3.copy_object(Bucket=targetBucket, Key=srcBucketObjKey, CopySource=copy_source)
		return response['ContentType']
	except Exception as err:
		print ("Error -"+str(err))
		return err