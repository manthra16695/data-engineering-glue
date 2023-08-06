import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1691176689028 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-moorthy/accelerometer/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1691176689028",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-moorthy/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node1691176822997 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1691176689028,
    keys1=["timeStamp"],
    keys2=["lastUpdateDate"],
    transformation_ctx="Join_node1691176822997",
)

# Script generated for node Drop Fields
DropFields_node1691176854546 = DropFields.apply(
    frame=Join_node1691176822997,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1691176854546",
)

# Script generated for node Amazon S3
AmazonS3_node1691176900444 = glueContext.getSink(
    path="s3://udacity-moorthy/accelerometer/machine_learning_trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1691176900444",
)
AmazonS3_node1691176900444.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_trusted"
)
AmazonS3_node1691176900444.setFormat("json")
AmazonS3_node1691176900444.writeFrame(DropFields_node1691176854546)
job.commit()
