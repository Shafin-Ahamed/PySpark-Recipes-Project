from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

recipe_schema = StructType(fields = [StructField("name", StringType(), True),
                                     StructField("ingredients", StringType(), True),
                                     StructField("url", StringType(), True),
                                     StructField("image", StringType(), True),
                                     StructField("cookTime", StringType(), True),
                                     StructField("recipeYield", StringType(), True),
                                     StructField("datePublished", DateType(), True),
                                     StructField("prepTime", StringType(), True),
                                     StructField("description", StringType(), True)])

