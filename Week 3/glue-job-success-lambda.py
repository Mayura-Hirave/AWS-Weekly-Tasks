import boto3
from custom_waiters import Waiter
		
class Table:
	def __init__(self, event, db_name = 'db-for-crawler'):
		self.path = event['Path']  #-- path of data store
		self.db_name = db_name     
		self.table_name = event['TableName']

	def create_table(self, glue, crawler_name='myCrawler'):
		try:
			if crawler_name not in glue.list_crawlers()['CrawlerNames']:
				print(crawler_name + "- This crawler doesn't exist")
				return False
			glue.update_crawler(Name=crawler_name, Targets={'S3Targets': [{'Path': self.path}]})
			glue.start_crawler(Name=crawler_name)
			crawl = Waiter.crawler_run_completed(Client=glue, CrawlerName=crawler_name)
			if crawl['Status'] != 'SUCCEEDED':
				print("Crawler Failed " + crawl['ErrorMessage'])
				return False
			return True
			
			"""
			table_name = None
			for table in glue.get_tables(DatabaseName=db_name)['table_list']:
				if table["StorageDescriptor"]["Location"] == path:
					table_name = table["Name"]
					break
			if table_name is None:
				raise Exception("Crawler doesnt created table for inputFile")
			"""
		except Exception as err:
			raise Exception("Error in Table.create_table():\n"+str(err))

	def run_query(self, athena, query_result_bucket='s3://athena-query-results-buckett/'):
		try:
			query_str = 'CREATE VIEW ' + self.table_name + '_view AS SELECT * FROM ' + self.table_name
			execution_id = athena.start_query_execution(QueryString=query_str,
										QueryExecutionContext={'Database': self.db_name, 'Catalog': 'AwsDataCatalog'},
										ResultConfiguration={'OutputLocation': query_result_bucket})['QueryExecutionId']
			status = Waiter.query_run_completed(Client=athena, QueryExecutionID=execution_id)
			if status['State'] == 'SUCCEEDED':
				print("Query successfully executed!")
				return True
			else:
				print("Query Failed. Reason: ", status['StateChangeReason'])
				return False
		except Exception as err:
			raise Exception("Error in Table.run_query():\n"+str(err))

def lambda_handler(event, context):
	try:
		table = Table(event)
		if table.create_table(boto3.client("glue")):
			if table.run_query(boto3.client("athena")):
				return True
		return False
	except Exception as err:
		raise Exception("Error in glue-job-success-lambda.lambda_handler():\n"+str(err))
