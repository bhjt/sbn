import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "transactional_data", table_name = "src_transactions", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "transactional_data", table_name = "src_transactions", transformation_ctx = "datasource0")
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "master_data", table_name = "src_customers", transformation_ctx = "datasource1")
## @type: ApplyMapping
## @args: [mapping = [("customer_id", "string", "customer_id", "string"), ("basket", "array", "basket", "array"), ("date_of_purchase", "string", "date_of_purchase", "string"), ("d", "string", "d", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
#applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("customer_id", "string", "customer_id", "string"), ("basket", "array", "basket", "array"), ("date_of_purchase", "string", "date_of_purchase", "string"), ("d", "string", "d", "string")], transformation_ctx = "applymapping1")
#applymapping2 = ApplyMapping.apply(frame = datasource1, mappings = [("customer_id", "string", "customer_id", "string"), ("loyalty_score", "long", "loyalty_score", "long")], transformation_ctx = "applymapping2")
## @type: SelectFields
## @args: [paths = ["customer_id", "basket", "date_of_purchase"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
#selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["customer_id", "basket", "date_of_purchase"], transformation_ctx = "selectfields2")
#selectfields3 = SelectFields.apply(frame = applymapping2, paths = ["customer_id", "loyalty_score"], transformation_ctx = "selectfields3")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "transactional_data", table_name = "src_transactions", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
#resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "transactional_data", table_name = "src_transactions", transformation_ctx = "resolvechoice3")
#resolvechoice4 = ResolveChoice.apply(frame = selectfields3, choice = "MATCH_CATALOG", database = "master_data", table_name = "src_customers", transformation_ctx = "resolvechoice4")
## @type: DataSink
## @args: [database = "transactional_data", table_name = "src_transactions", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = resolvechoice3]
#datasink4 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice3, database = "transactional_data", table_name = "src_transactions", transformation_ctx = "datasink4")
#datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "master_data", table_name = "src_customers", transformation_ctx = "datasink5")
##Converting Dynamic DataFrame to DataFrame
#salesDF = datasink4.toDF()
#customerDF = datasink5()
#sales_cust = salesDF.join(customerDF on salesDF.customer_id = CustomerDF.customer_id, how='left_outer')
#sales_cust = join.apply(datasource0, datasource1, keys1 = customer_id, keys2 = customer_id, transformation_ctx = "sales_cust")
## @type: Join
## @args: [keys1 = [<keys1>], keys2 = [<keys2>]]
## @return: <output>
## @inputs: [frame1 = <frame1>, frame2 = <frame2>]
sales_cust = Join.apply(frame1 = datasource0, frame2 = datasource1, keys1 = ["customer_id"], keys2 = ["customer_id"], transformation_ctx = "sales_cust")
#datasink = glueContext.write_dynamic_frame.from_catalog(frame = sales_cust, database = "sales_target", table_name = "trg_transactions", transformation_ctx = "datasink")
datasink = glueContext.write_dynamic_frame.from_options(frame = sales_cust, connection_type = "s3", connection_options = {"path": "s3://targetbucket01"}, format = "csv", transformation_ctx = "datasink")
job.commit()
## @type: Join
## @args: [keys1 = [<keys1>], keys2 = [<keys2>]]
## @return: <output>
## @inputs: [frame1 = <frame1>, frame2 = <frame2>]
#<output> = Join.apply(frame1 = <frame1>, frame2 = <frame2>, keys1 = [<keys1>], keys2 = [<keys2>], transformation_ctx = "<transformation_ctx>")
