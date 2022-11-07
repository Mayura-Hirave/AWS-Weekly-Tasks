"""
All of Boto3's resource and Client classes are generated at runtime.
This means that you cannot directly inherit and then extend the functionality of these classes
    because they do not exist until the program actually starts running.
"""
import time


class Waiter:
    @staticmethod
    def job_run_completed(Client, JobName, RunId, max_attempts=3, delay=50):
        num_attempts = 0
        while True:
            time.sleep(delay)
            response = Client.get_job_run(JobName=JobName, RunId=RunId)
            if response['JobRun']['JobRunState'] in ('SUCCEEDED', 'FAILED'):
                return response['JobRun']
            num_attempts += 1
            if num_attempts == max_attempts:
                raise Exception("Max attempts exceeded. Current State: " + response['JobRun']['JobRunState'])
            

    @staticmethod
    def crawler_run_completed(Client, CrawlerName, max_attempts=3, delay=60):
        num_attempts = 0
        while True:
            time.sleep(delay)
            crawler = Client.get_crawler(Name=CrawlerName)['Crawler']
            if crawler['State'] == 'READY':
                return crawler['LastCrawl']
            num_attempts += 1
            if num_attempts == max_attempts:
                raise Exception("Max attempts exceeded. Current State: " + crawler['State'])

    @staticmethod
    def query_run_completed(Client, QueryExecutionID, max_attempts=3, delay=5):
        num_attempts = 0
        while True:
            time.sleep(delay)
            status = Client.get_query_execution(QueryExecutionId=QueryExecutionID)['QueryExecution']['Status']
            if status['State'] in ('SUCCEEDED', 'FAILED'):
                return status
            num_attempts += 1
            if num_attempts == max_attempts:
                raise Exception("Max attempts exceeded. Current State: " + status['State'])

