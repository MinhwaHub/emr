import os

file_name = os.path.basename(__file__)
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from src.constant.cross_wallet import cross_wallet_raw
from src.utils import (
    MySQLReader,
    get_job_info,
    S3Handler,
    repair_glue_partitions,
)

job = get_job_info(file_name)
# data_layer = job["data_layer"]
# schema_name = file_name.split("dl_")[1].split(".py")[0]
schema_name = "cross_wallet"
start_date = job["start_date"]
end_date = job["end_date"]
batch_bdate = job["batch_bdate"]

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test_pipeline").getOrCreate()

for dataset in cross_wallet_raw:
    ## except token table
    if dataset["Table"] == "token":
        data_layer = "dim"
    else:
        data_layer = "dl"
    ##

    db_reader = MySQLReader(spark, dataset["db_config"])
    df = db_reader.read_table(
        table_name=f"{dataset['Table']}",
        select_field=dataset["SelectField"],
        query=dataset.get("Query", None),
        time_field=dataset.get("TimeField", None),
        start_date=start_date,
        end_date=end_date,
    )
    # if isinstance(df, DataFrame) and df.count() > 0:
    #     df.limit(3).show()
    print(df)

    s3_writer = S3Handler()

    try:
        s3_writer.write_table(
            df=df,
            data_layer=data_layer,
            schema_name=schema_name,
            table_name=dataset["Table"],
            partition_cols=dataset.get("PartitionCols", ["dt_utc"]),
            mode=dataset.get("Mode", "append"),
        )
        print(f"S3 write success \n")
        s3_writer.table_manager(
            spark,
            schema_name,
            dataset["Table"],
            batch_bdate,
            start_date,
            end_date,
            status=1,
        )
        print(f"table_manager success \n")
        repair_glue_partitions(spark, schema_name, data_layer, dataset["Table"])

    except Exception as e:
        print(f"S3 write failed: {e}")

        s3_writer.table_manager(
            spark,
            schema_name,
            dataset["Table"],
            batch_bdate,
            start_date,
            end_date,
            status=0,
        )
        print(f"table_manager success \n")
