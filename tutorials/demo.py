"""
usage :
---

compare-tables --temporary_gcs_bucket c4-gdw-pss-staging-bucket-660545939213 --base_table_id c4-gdw-sbx.ariba_sync.Invoice --compare_table_id c4-gdw-sbx.ariba_sync.InvoiceApprovalRequests --target_table_id c4-gdw-sbx.test.gdw_tables_auditing --join_columns [('InitialUniqueName', 'UniqueName')]
"""

from pyspark.sql import SparkSession

from ingestor.comparator import DfComparator

spark = SparkSession.builder.appName('example_compare_dfs').getOrCreate()

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('custom_pyspark') \
    .getOrCreate()

source_df = spark.read.csv("data/events/source.csv", header=True, inferSchema="true")
source_df = source_df.toDF(*[c.lower() for c in source_df.columns])

target_df = spark.read.csv("data/events/target.csv", header=True, inferSchema="true")
target_df = target_df.toDF(*[c.lower() for c in target_df.columns])

# source_df.printSchema()
source_df.show(truncate=False)

# target_df.printSchema()
target_df.show(truncate=False)

comparison = DfComparator(
    spark,
    source_df,
    target_df,
    base_table='c4-gdw-sbx.ariba_sync.Invoice',
    compare_table='c4-gdw-sbx.ariba_sync.InvoiceApprovalRequests',
    target_table='c4-gdw-sbx.test.gdw_tables_auditing',
    temporary_gcs_bucket='c4-gdw-pss-staging-bucket-660545939213',
    join_columns=[('id', 'target_id'), ('ordr_id', 'ordr_id')],
    # extra_transforms=[{'lpad col1', 'where id not null'}]
)

comparison.load_table()
