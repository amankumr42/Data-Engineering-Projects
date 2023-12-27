from pyspark.sql import SparkSession
import boto3

def create_spark_session(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()

def archive_landing_zone_file(boto3_client, bucket_name, path_prefix, archive_path):   
    """_summary_

    Args:
        boto3_client (boto3_client): boto3_client
        bucket_name (string): bucket_name
        path_prefix (string): landing_zone
        archive_path (string): moving file from to stg path

    Returns:
        List : List of new file found in landing zone
    """

    file_list = []
    response = boto3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=path_prefix
    )

    # This only lists the first 1000 keys
    for content in response.get('Contents', []):
        if "." in content['Key']:
            file_list.append(str(content['Key']))

    # Move the file list to destination location
    for files in file_list:
        copy_source = {
            "Bucket": bucket_name,
            "Key": files
        }        
        boto3.copy_object(Bucket = bucket_name, CopySource = copy_source, key = f"{archive_path}/{files}")

    return file_list    

def spark_jobs(spark, bucket_name, landing_zone, refined_path):
    # Create DataFrame
    df = spark.read.csv(f"s3://{bucket_name}/{landing_zone}/*" , header=True)

    # convert csv to parquet
    df.write.parquet(f"s3://{bucket_name}/{refined_path}", mode="append")

if __name__ == "__main__":
    client = boto3.client("s3")
    app_name = "spark_emr_serverless_example"
    bucket_name = None
    landing_zone = None
    archive_path = None
    refined_path = None
    new_files = archive_landing_zone_file(client, bucket_name, landing_zone, archive_path)
    
    # Create Spark Session
    spark = create_spark_session(app_name=app_name)

    # Start Spark ETL jobs
    spark_jobs(spark, bucket_name, landing_zone, refined_path) 
