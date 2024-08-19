from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local[1]") \
                    .appName("test") \
                    .getOrCreate()


recipe_000_input_path = "input_files/prod_data/recipes-000.json"
recipe_001_input_path = "input_files/prod_data/recipes-001.json"
recipe_002_input_path = "input_files/prod_data/recipes-002.json"

recipes_output_path = "output_files/prod_data_output"






