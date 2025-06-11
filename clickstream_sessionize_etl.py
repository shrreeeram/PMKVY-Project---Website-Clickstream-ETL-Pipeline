import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Required Args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ✅ Step 1: Read Raw JSON Data from S3
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3",
    connection_options = {"paths": ["s3://g7-clickstream-raw-logs/"]},
    format = "json"
)

# ✅ Step 2: Transform Data (Example: Select some fields)
transformed_df = ApplyMapping.apply(
    frame = datasource0,
    mappings = [
        ("session_id", "string", "session_id", "string"),
        ("user_id", "string", "user_id", "string"),
        ("page_visited", "string", "page_visited", "string"),
        ("timestamp", "string", "timestamp", "string")
    ]
)

# ✅ Step 3: Write Output to Processed S3 Bucket
datasink = glueContext.write_dynamic_frame.from_options(
    frame = transformed_df,
    connection_type = "s3",
    connection_options = {"path": "s3://g7-clickstream-processed-output/"},
    format = "json"
)

job.commit()