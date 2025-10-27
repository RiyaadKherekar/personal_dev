import sys, json, logging, boto3
from datetime import datetime
import pytz
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import rdbms_based_lib 
from awsglue.transforms import *

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
logger = logging.getLogger()

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "s3_bucket",
    "main_prefix",
    "dest_root",
    "partition_col",
    "tables",
    "environment",
    "region_name"
])

s3_bucket = args["s3_bucket"]                    
main_prefix = args["main_prefix"]      
dest_root = args["dest_root"]        
partition_col = args["partition_col"]                        
tables = json.loads(args["tables"]) # tables will typically be a JSON array                
environment = args["environment"]
region_name = args["region_name"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sql("SET spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
spark.sql("SET spark.sql.legacy.parquet.int96RebaseModeInWrite=CORRECTED")
spark.sql("SET spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED")
spark.sql("SET spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.files.openCostInBytes",   1 * 1024 * 1024)   # 1MB (default is 4MB; start with 1MB)
spark.conf.set("spark.sql.files.maxPartitionBytes", 1024 * 1024 * 1024)  # 1GB
spark.conf.set("spark.sql.files.minPartitionNum", "8")

spark.sql("SET spark.sql.shuffle.partitions").show(truncate=False)
spark.sql("SET spark.sql.adaptive.enabled").show(truncate=False)
spark.sql("SET spark.sql.files.openCostInBytes").show(truncate=False)
spark.sql("SET spark.sql.files.maxPartitionBytes").show(truncate=False)
spark.sql("SET spark.sql.files.minPartitionNum").show(truncate=False)

sa_tz = pytz.timezone("Africa/Johannesburg")
run_start_dt = datetime.now(sa_tz).strftime("%Y-%m-%d %H:%M:%S")

account_id = rdbms_based_lib.get_account_id()
logger.info(f"account_id: {account_id}")
sns_success_topic_arn = f'arn:aws:sns:eu-west-1:{account_id}:{environment}-ingestion-success' 
sns_failure_topic_arn = f'arn:aws:sns:eu-west-1:{account_id}:{environment}-pipeline-failures' 
sns_client = boto3.client('sns', region_name='eu-west-1')

for table in tables:
    base = f"s3://{s3_bucket}/{main_prefix}{table}"
    df = (spark.read
            .option("basePath", base)
            .parquet(f"{base}/{partition_col}=*"))

    print(f"Table: {table}")
    df.printSchema()
    print()

    reducer = df.coalesce(1)

    output = f"{dest_root}{table}/"
    (reducer.write
            .mode("overwrite")
            .partitionBy(partition_col)
            .parquet(output))

    print(f"Wrote (partitioned by {partition_col}) for {table} -> {output}")

run_end_dt = datetime.now(sa_tz).strftime("%Y-%m-%d %H:%M:%S")
logger.info(f"Done. Start={run_start_dt} End={run_end_dt}")
job.commit()
