import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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
AmazonS3_node1760312363314 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-yazeedalmuhlaki/step_trainer/trusted/data/"], "recurse": True}, transformation_ctx="AmazonS3_node1760312363314")

# Script generated for node Amazon S3
AmazonS3_node1760312363921 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-yazeedalmuhlaki/accelerometer/trusted/data/"], "recurse": True}, transformation_ctx="AmazonS3_node1760312363921")

# Script generated for node SQL Query
SqlQuery4395 = '''
SELECT
    st.*,
    at.user,
    at.timeStamp,
    at.x,
    at.y,
    at.z
FROM
    step_trainer_trusted st
INNER JOIN
    accelerometer_trusted at
ON
    st.sensorReadingTime = at.timeStamp;
'''
SQLQuery_node1760312599893 = sparkSqlQuery(glueContext, query = SqlQuery4395, mapping = {"step_trainer_trusted":AmazonS3_node1760312363314, "accelerometer_trusted":AmazonS3_node1760312363921}, transformation_ctx = "SQLQuery_node1760312599893")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1760312599893, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760312279639", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1760312722836 = glueContext.getSink(path="s3://stedi-lakehouse-yazeedalmuhlaki/machine_learning/curated/data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1760312722836")
AmazonS3_node1760312722836.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
AmazonS3_node1760312722836.setFormat("json")
AmazonS3_node1760312722836.writeFrame(SQLQuery_node1760312599893)
job.commit()