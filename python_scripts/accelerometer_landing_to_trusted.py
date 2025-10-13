import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1760303157515 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-yazeedalmuhlaki/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1760303157515")

# Script generated for node Amazon S3
AmazonS3_node1760303228206 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-yazeedalmuhlaki/customer/trusted/data/"]}, transformation_ctx="AmazonS3_node1760303228206")

# Script generated for node Join
Join_node1760303276642 = Join.apply(frame1=AmazonS3_node1760303157515, frame2=AmazonS3_node1760303228206, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1760303276642")

# Script generated for node Select Fields
SelectFields_node1760304155973 = SelectFields.apply(frame=Join_node1760303276642, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="SelectFields_node1760304155973")

# Script generated for node Amazon S3
AmazonS3_node1760303398451 = glueContext.getSink(path="s3://stedi-lakehouse-yazeedalmuhlaki/accelerometer/trusted/data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1760303398451")
AmazonS3_node1760303398451.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
AmazonS3_node1760303398451.setFormat("json")
AmazonS3_node1760303398451.writeFrame(SelectFields_node1760304155973)
job.commit()