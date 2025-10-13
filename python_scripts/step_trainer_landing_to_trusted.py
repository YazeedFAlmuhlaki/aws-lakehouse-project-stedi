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

# Script generated for node customer_curated
customer_curated_node1760310764467 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-yazeedalmuhlaki/customer/curated/data/"], "recurse": True}, transformation_ctx="customer_curated_node1760310764467")

# Script generated for node step_trainer_landing
step_trainer_landing_node1760310763813 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-yazeedalmuhlaki/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1760310763813")

# Script generated for node SQL Query
SqlQuery4353 = '''
SELECT
    st.sensorReadingTime,
    st.serialNumber,
    st.distanceFromObject
FROM
    step_trainer_landing st
INNER JOIN
    customer_curated cc
ON
    st.serialNumber = cc.serialNumber;
'''
SQLQuery_node1760311218451 = sparkSqlQuery(glueContext, query = SqlQuery4353, mapping = {"step_trainer_landing":step_trainer_landing_node1760310763813, "customer_curated":customer_curated_node1760310764467}, transformation_ctx = "SQLQuery_node1760311218451")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1760311218451, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1760310694352", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1760311787584 = glueContext.getSink(path="s3://stedi-lakehouse-yazeedalmuhlaki/step_trainer/trusted/data/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1760311787584")
AmazonS3_node1760311787584.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
AmazonS3_node1760311787584.setFormat("json")
AmazonS3_node1760311787584.writeFrame(SQLQuery_node1760311218451)
job.commit()