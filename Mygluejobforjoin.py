import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # Convert the input DynamicFrame to a DataFrame
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Create a temporary view for the DataFrame
    df.createOrReplaceTempView("inputTable")

    # Apply the transformations using Spark SQL
    df_transformed = spark.sql(
        """
        SELECT UPPER(language) AS language_upper,
               UPPER(name) AS name_upper,
               'Y' AS status
        FROM inputTable
    """
    )

    # Convert the transformed DataFrame to a DynamicFrame
    dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "result0")

    # Return the transformed DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform0": dyf_transformed}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon JSON S3
AmazonJSONS3_node1693647636810 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://myglueproject1/json_input/"], "recurse": True},
    transformation_ctx="AmazonJSONS3_node1693647636810",
)

# Script generated for node Amazon CSV  S3
AmazonCSVS3_node1693647467355 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://myglueproject1/csv_input/"], "recurse": True},
    transformation_ctx="AmazonCSVS3_node1693647467355",
)

# Script generated for node Join
Join_node1693647868443 = Join.apply(
    frame1=AmazonJSONS3_node1693647636810,
    frame2=AmazonCSVS3_node1693647467355,
    keys1=["id"],
    keys2=["id"],
    transformation_ctx="Join_node1693647868443",
)

# Script generated for node Custom Transform
CustomTransform_node1693648285027 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"Join_node1693647868443": Join_node1693647868443}, glueContext
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1693648955052 = SelectFromCollection.apply(
    dfc=CustomTransform_node1693648285027,
    key=list(CustomTransform_node1693648285027.keys())[0],
    transformation_ctx="SelectFromCollection_node1693648955052",
)

# Script generated for node Amazon S3
AmazonS3_node1693649066316 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1693648955052,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://myglueproject1/join_output/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1693649066316",
)

job.commit()
