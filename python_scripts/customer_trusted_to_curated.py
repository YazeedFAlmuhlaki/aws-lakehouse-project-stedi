import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1760304937066 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-yazeedalmuhlaki/accelerometer/trusted/data/"], "recurse": True}, transformation_ctx="AmazonS3_node1760304937066")

# Script generated for node Amazon S3
AmazonS3_node1760304936474 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-yazeedalmuhlaki/customer/trusted/data/"], "recurse": True}, transformation_ctx="AmazonS3_node1760304936474")

# Script generated for node Join
Join_node1760305040494 = Join.apply(frame1=AmazonS3_node1760304936474, frame2=AmazonS3_node1760304937066, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1760305040494")

# Script generated for node Drop Fields
DropFields_node1760305344667 = DropFields.apply(frame=Join_node1760305040494, paths=["z", "y", "user", "x", "timestamp"], transformation_ctx="DropFields_node1760305344667")

# Script generated for node Drop Duplicates
DropDuplicates_node1760306668452 =  DynamicFrame.fromDF(DropFields_node1760305344667.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1760306668452")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1760306668452, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760304839897", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1760305400538 = glueContext.getSink(path="s3://stedi-lakehouse-yazeedalmuhlaki/customer/curated/data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1760305400538")
AmazonS3_node1760305400538.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customers_curated")
AmazonS3_node1760305400538.setFormat("json")
AmazonS3_node1760305400538.writeFrame(DropDuplicates_node1760306668452)
job.commit()