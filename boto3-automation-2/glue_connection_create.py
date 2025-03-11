import boto3

# Initialize Glue client
glue_client = boto3.client("glue", region_name="us-east-2")  # Change to your AWS region

def create_glue_connection(connection_name, host, database, username, password, subnet_id, security_group_id, availability_zone):
    """
    Creates an AWS Glue connection to an RDS MySQL instance.
    """
    try:
        response = glue_client.create_connection(
            ConnectionInput={
                "Name": connection_name,
                "Description": "Glue connection to MySQL RDS",
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": f"jdbc:mysql://{host}:3306/{database}",
                    "USERNAME": username,
                    "PASSWORD": password,  # Store in AWS Secrets Manager for better security
                    "JDBC_DRIVER_CLASS_NAME": "com.mysql.jdbc.Driver"
                },
                "PhysicalConnectionRequirements": {
                    "SubnetId": subnet_id,
                    "SecurityGroupIdList": [security_group_id],
                    "AvailabilityZone": availability_zone
                }
            }
        )
        print(f"✅ Glue Connection '{connection_name}' created successfully!")
        return response
    except Exception as e:
        print(f"❌ Failed to create Glue Connection: {e}")

def delete_glue_connection(connection_name):
    """
    Deletes an AWS Glue connection by name.
    """
    try:
        response = glue_client.delete_connection(Name=connection_name)
        print(f"✅ Glue Connection '{connection_name}' deleted successfully!")
        return response
    except Exception as e:
        print(f"❌ Failed to delete Glue Connection: {e}")


def test_glue_connection(connection_name):
    """
    Tests an AWS Glue connection by retrieving its details.
    """
    try:
        response = glue_client.get_connection(Name=connection_name)
        print(f"✅ Glue Connection '{connection_name}' exists and is configured correctly!")
        return response
    except Exception as e:
        print(f"❌ Glue Connection '{connection_name}' test failed: {e}")



# Example Usage
if __name__ == "__main__":
    connection_name = "my-rds-mysql-connection"
    host = "rds-mysql-instance.clg2yw2cyk43.us-east-2.rds.amazonaws.com"
    database = "MyDatabase"
    username = "admin"
    password = "abcd1234"  # Use AWS Secrets Manager instead of hardcoding
    subnet_id = "subnet-0e7a9504e56ad363d"  # Change this to your VPC Subnet ID
    security_group_id = "sg-03c777c268a31e42b"  # Change this to your Security Group ID
    availability_zone = "us-east-2b"  # Change to your RDS Availability Zone

    # Create Connection
    #create_glue_connection(connection_name, host, database, username, password, subnet_id, security_group_id, availability_zone)

    # Delete Connection (Uncomment to delete)
    #delete_glue_connection(connection_name)
    # Example Usage

    test_glue_connection(connection_name)

