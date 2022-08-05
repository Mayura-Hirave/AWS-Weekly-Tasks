import boto3
import urllib.parse

def lambda_handler(event, context):
	srcBucketName = urllib.parse.unquote(event['Records'][0]['s3']['bucket']['name'])
	srcBucketObjKey = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
	
	destinationBucket = 'backup-s3buckett1'
	copy_source = {'Bucket': srcBucketName, 'Key': srcBucketObjKey }
	try:
		s3 = boto3.client('s3')
		s3.copy_object(Bucket=destinationBucket, Key=srcBucketObjKey, CopySource=copy_source)
	except Exception as err:
		print ("Error -"+str(err))
