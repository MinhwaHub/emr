import os

file_name = os.path.basename(__file__)

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from src.constant import cross_dex_raw
from src.utils import (
    MySQLReader,
    get_job_info,
    S3Handler,
    repair_glue_partitions,
)

### set parameters
job = get_job_info(file_name)
data_layer = job["data_layer"]
schema_name = file_name.split("dl_")[1].split(".py")[0]
start_date = job["start_date"]
end_date = job["end_date"]
batch_bdate = job["batch_bdate"]
###


def main(spark):
    for dataset in cross_dex_raw:
        db_reader = MySQLReader(spark, dataset["db_config"])
        df = db_reader.read_table(
            table_name=f"{dataset['Table']}",
            select_field=dataset["SelectField"],
            time_field=dataset.get("TimeField", None),
            start_date=start_date,
            end_date=end_date,
            mode=dataset.get("Mode", "append"),
        )
        if isinstance(df, DataFrame) and df.count() > 0:
            df.limit(3).show()

            s3_writer = S3Handler()

            try:
                s3_writer.write_table(
                    df=df,
                    data_layer=data_layer,
                    schema_name=schema_name,
                    table_name=dataset["Table"],
                    partition_cols=dataset.get("PartitionCols", ["dt_utc"]),
                    format=dataset.get("Format", "parquet"),
                    mode=dataset.get("Mode", "append"),
                )
                print(f">>> S3 write SUCCESS \n")
                s3_writer.table_manager(
                    spark,
                    schema_name,
                    dataset["Table"],
                    batch_bdate,
                    start_date,
                    end_date,
                    status=1,
                )
                print(f">>> table_manager SUCCESS \n")
                repair_glue_partitions(spark, schema_name, data_layer, dataset["Table"])

            except Exception as e:
                print(f">>> S3 write FAILED: {e}")

                s3_writer.table_manager(
                    spark,
                    schema_name,
                    dataset["Table"],
                    batch_bdate,
                    start_date,
                    end_date,
                    status=0,
                )
                print(f">>> Table Manager SUCCESS \n")

    # spark.stop()
