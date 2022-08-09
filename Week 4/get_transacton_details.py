import json
import boto3
import pymysql
import urllib.parse


class BankDB:
    def __init__(self):
        try:
            rds = boto3.client('rds')
            db_details = rds.describe_db_instances(DBInstanceIdentifier='mysql-db-instance')
            db_endpoint = db_details['DBInstances'][0]['Endpoint']['Address']
            secret_manager = boto3.client(service_name='secretsmanager', region_name='ap-south-1')
            user_credentials = json.loads(
                secret_manager.get_secret_value(SecretId='masteruser_credentials')['SecretString'])
            db_username = user_credentials["username"]
            db_password = user_credentials["password"]

            self.conn = pymysql.connect(host=db_endpoint, user=db_username, passwd=db_password)
            self.cursor = self.conn.cursor()
            self.cursor.execute("use Bank;")
        except Exception as e:
            raise Exception('Exception in BankDB.__init__(): ' + str(e))

    def get_username(self, customer_id):
        try:
            query = f"SELECT full_name FROM Customer WHERE id={customer_id}"
            self.cursor.execute(query)
            username = self.cursor.fetchall()[0][0]
            return username
        except Exception as e:
            raise Exception('Exception in BankDB.get_username(): ' + str(e))

    def check_user_credentials(self, customer_id, password):
        try:
            query = f"SELECT COUNT(id) FROM Customer \
                        WHERE id ={customer_id} AND password = MD5('{password}');"
            self.cursor.execute(query)
            return False if self.cursor.fetchall()[0][0] != 1 else True
        except Exception as e:
            raise Exception('Exception in BankDB.check_user_credentials(): ' + str(e))

    def get_transaction_count(self, customer_id, year, month):
        try:
            query = f"SELECT COUNT(id) FROM Transactions \
                        WHERE account_id IN ( SELECT id FROM Account WHERE customer_id={customer_id}) \
                              AND YEAR(datetime)={year} \
                              AND MONTH(datetime)={month};"
            self.cursor.execute(query)
            count = self.cursor.fetchall()[0][0]
            return count
        except Exception as e:
            raise Exception('Exception in BankDB.get_transaction_count(): ' + str(e))


class Util:
    @staticmethod
    def get_lambda_response(message):
        response = {'statusCode': 200, 'body': None, "headers": {"Content-Type": "text/html"}}
        webpage_first_part = '<html> <head> <title>Client Home Page</title> </head>\
                              <style> body {font-family: Arial, Helvetica, sans-serif;} * {box-sizing: border-box} \
                              hr { border: 1px solid #f1f1f1; margin-bottom: 25px; } \
                              button { background-color: #04AA6D; color: white; padding: 14px 20px; margin: 8px 0;\
                              border: none; cursor: pointer; width: 100%; opacity: 0.9; } button:hover { opacity:1; } \
                              .container1 { padding: 16px; width: 40%; margin: auto; border:1px solid #ccc; \
                              text-align: center;} .container2 { padding: 16px; width: 100%; border:1px solid #ccc; \
                              margin: auto; text-align: left; } </style> <body> <div class="container1"> \
                              <h1>Welcome to ABC Bank </h1> <hr> <div class="container2"> '
        webpage_last_part = ' <a href="http://abc-bank.s3-website.ap-south-1.amazonaws.com"> \
                            <button type="submit"> Go To Homepage</button> </a> </div> </div> </body> </html>'
        response['body'] = webpage_first_part + message + webpage_last_part
        return response


def lambda_handler(event, context):
    try:
        if not event['body']:
            return Util.get_lambda_response('<h2>Invalid Request!</h2>')

        # Extracting data collected through html form
        form_details = {"customer_id": None, "password": None, "is_valid_req": None, "month": None, "year": None}
        for string in event['body'].split('&'):
            list1 = string.split('=')
            form_details[list1[0]] = list1[1]
        form_details["password"] = urllib.parse.unquote_plus(form_details["password"])

        # checking validity of API Invocation
        if form_details['is_valid_req'] != "True":
            return Util.get_lambda_response('<h2>Invalid Request!</h2>')

        bank_db = BankDB()
        if bank_db.check_user_credentials(form_details['customer_id'], form_details['password']) is False:
            return Util.get_lambda_response('<h2>Customer ID or Password is incorrect!</h2>')

        username = bank_db.get_username(form_details['customer_id'])
        count = bank_db.get_transaction_count(form_details['customer_id'], form_details['year'], form_details['month'])
        return Util.get_lambda_response(f"<h2>Hi {username}!</h2> <h3>Total Number of Transactions: {count}</h3>")
    except Exception as e:
        print(e)
        return Util.get_lambda_response("<h2>Internal Server Error!</h2>")
