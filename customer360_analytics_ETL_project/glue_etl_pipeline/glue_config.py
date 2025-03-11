import os

# S3 Paths
S3_SOURCE_PATH = os.getenv("S3_SOURCE_PATH", "s3://mybucket31101999/raw-data/")
S3_TARGET_PATH = "s3://mybucket31101999/analytics"

# Glue Catalog Database & Table
GLUE_DATABASE = "customer_analytics"
GLUE_TABLE = "customer_transactions"


USER_MYSQL_URL = "jdbc:mysql://rds-mysql-instance.clg2yw2cyk43.us-east-2.rds.amazonaws.com:3306/UserService"
ORDER_MYSQL_URL = "jdbc:mysql://rds-mysql-instance.clg2yw2cyk43.us-east-2.rds.amazonaws.com:3306/OrderService"
PRODUCT_MYSQL_URL = "jdbc:mysql://rds-mysql-instance.clg2yw2cyk43.us-east-2.rds.amazonaws.com:3306/ProductService"

MYSQL_PROPERTIES = {
    'user': 'admin',
    'password': 'abcd1234',
    'driver': 'com.mysql.cj.jdbc.Driver',
    'allowPublicKeyRetrieval':'true',
    'useSSL':'false'
}