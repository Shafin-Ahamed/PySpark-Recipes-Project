import datetime
import pyspark
import pandas as pd
import os
from pyspark.sql import functions as F
from datetime import datetime
from configs import *
from common_schemas import *


@F.pandas_udf("int")
def parse_iso8601_duration(str_duration : pd.Series) -> pd.Series:
    return str_duration.apply(lambda duration: (pd.Timedelta(duration).seconds / 60))


def extract_and_transform_data(path:str):
    df = spark.read.schema(recipe_schema).json(path)
    return df.select(F.col("name"),
                     F.col("ingredients"),
                     F.col("url"),
                     F.col("image"),
                     F.col("cookTime"),
                     F.col("recipeYield"),
                     F.col("datePublished"),
                     F.col("prepTime"),
                     F.col("description")) \
             .filter((df.ingredients.like('%beef%')) | (df.ingredients.like('%Beef%'))) \
             .withColumn("cookTime_in_minutes", parse_iso8601_duration(F.col("cookTime"))) \
             .withColumn("prepTime_in_minutes", parse_iso8601_duration(F.col("prepTime")))

def add_totals(df):
    return df.withColumn("Total_Cooking_Time", df.cookTime_in_minutes + df.prepTime_in_minutes)

def to_sql(df):
    df.createOrReplaceTempView('Table')
    return spark.sql("SELECT *, CASE WHEN Total_Cooking_Time < 30 THEN 'Easy' WHEN Total_Cooking_Time BETWEEN 30 AND 60 THEN 'Medium' WHEN Total_Cooking_Time > 60 THEN 'Hard' END AS Difficulty FROM Table")

def to_sql_2(df):
    df.createOrReplaceTempView('Table2')
    return spark.sql("SELECT Difficulty, ROUND(AVG(Total_Cooking_Time),2) AS Average_Cooking_Time_Per_Difficulty FROM Table2 GROUP BY Difficulty")

def load_data(df, path):
    df.write.mode("overwrite").csv(path)

def run_etl(input_path, output_path):
    try:
        df_transformed = extract_and_transform_data(input_path)
        print('successfully loaded file :)')
        df_added = add_totals(df_transformed)
        df_sql = to_sql(df_added)
        print('successfully loaded SQL temp view 1 :))')
        results_df = to_sql_2(df_sql)
        print('successfully loaded SQL temp view 2 :D')
        load_data(results_df, output_path)
        print('load successful xD')
        print(results_df.show())
        return(results_df)
    except pyspark.errors.exceptions.captured.AnalysisException:
        print('Invalid path entered, pelase try again')