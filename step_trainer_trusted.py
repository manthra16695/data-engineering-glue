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
AmazonS3_node1691278512722 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://steptrainer-landing-moorthy/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1691278512722",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-moorthy/accelerometer/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node1691278562288 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1691278512722,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1691278562288",
)

# Script generated for node Drop Fields
DropFields_node1691278680894 = DropFields.apply(
    frame=Join_node1691278562288,
    paths=[
        "birthDay",
        "shareWithPublicAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "serialNumber",
        "shareWithResearchAsOfDate",
    ],
    transformation_ctx="DropFields_node1691278680894",
)

# Script generated for node Amazon S3
AmazonS3_node1691279262331 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1691278680894,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-moorthy/accelerometer/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1691279262331",
)

job.commit()
