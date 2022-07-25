import boto3


def lambda_handler(event, context):
    sns = boto3.client('sns')
    topic_arn = 'arn:aws:sns:us-west-1:478055296570:GlueJobFailureEmailNotification'
    print("GlueJob ", event["GlueJobName"], " Failed!")
    print("Error: ", event["ErrorDetails"])
    message = "Failed to convert " + event["FileName"] + " into parque file."
    sns.publish(TopicArn=topic_arn, Message=message, Subject='File conversion failed')
