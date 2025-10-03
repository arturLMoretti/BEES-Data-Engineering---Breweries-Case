from time import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, concat, lit


class BreweryDataTransformer:
    '''
    Class responsible for transforming brewery data from bronze to silver to gold layers
    '''
    
    def __init__(self, app_name: str = "BreweryDataTransformation", bronze_path: str = "./data/bronze"):
        self.bronze_path = bronze_path
        self.silver_path = "./data/silver"
        self.gold_path = "./data/gold"
        
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
    
    def get_brewery_data(self, path: str = None):
        if path is None:
            path = self.bronze_path
            
        df_bronze = self.spark.read.option("recursiveFileLookup", "true")\
          .json(f"{path}")
        
        initial_count = df_bronze.count()
        print(f"Initial count: {initial_count}")
        t_i = time()
        
        df_bronze = df_bronze\
          .filter(
            (col("id").isNotNull()) & (trim(col("id")) != "") &\
            (col("name").isNotNull()) & (trim(col("name")) != "") &\
            (col("city").isNotNull()) & (trim(col("city")) != "") &\
            (col("state").isNotNull()) & (trim(col("state")) != "") &\
            (col("country").isNotNull()) & (trim(col("country")) != "")
          )
        
        df_bronze.select("*").show(5)
        final_count = df_bronze.count()
        print(f"Final count: {final_count}")
        t_f = time()
        print(f"Time taken: {t_f - t_i} seconds")
        return df_bronze

    def deduplicate_data(self, df):
        initial_count = df.count()
        print(f"Before deduplication: {initial_count} records")

        df_dedup = df.dropDuplicates(["id"])
        final_count = df_dedup.count()

        print(f"After deduplication: {final_count} records")
        print(f"Duplicates removed: {initial_count - final_count}")
        
        return df_dedup

    def partition_data_by_location(self, df):
        df_partitioned = df.withColumn("location_partition", 
                                      concat(col("country"), lit("_"), col("state"))
                                      )
        return df_partitioned

    def save_transformed_data(self, df, path: str = None):
        if path is None:
            path = self.silver_path
            
        df.write.mode("overwrite").partitionBy("location_partition").parquet(path)
        print(f"Data saved to {path}")

    def transform_data_to_silver(self):
        data = self.get_brewery_data()
        data = self.deduplicate_data(data)
        data = self.partition_data_by_location(data)
        self.save_transformed_data(data)

    def aggregate_data_by_type_country_state(self, df):

        df_agg = df.select("brewery_type", "state", "country")\
          .groupBy("brewery_type", "state", "country")\
          .count()
        return df_agg

    def transform_data_to_gold(self):

        df_gold = self.spark.read.parquet(self.silver_path)
        df_type_country_state = self.aggregate_data_by_type_country_state(df_gold)
        df_type_country_state.write.mode("overwrite").parquet(self.gold_path)
        print(f"View type_country_state saved to {self.gold_path}")
    
    def stop_spark_session(self):
        self.spark.stop()

if __name__ == "__main__":
    transformer = BreweryDataTransformer()
    
    try:
        transformer.transform_data_to_silver()
        
        transformer.transform_data_to_gold()
    finally:
        transformer.stop_spark_session()