# recipes-etl-project

# Overview
The goal of this project is to perform a standard ETL of some sample cooking recipes. I will be taking JSON files and transforming them, then subsequently writing them to an output folder, all within the same local environment. I used a docker container so that I can procure Spark within my environment.

# Approach
1. Extract data from JSON files
2. Load them into PySpark frames
3. Transformations - Add columns to convert ISO 8601 duration to minutes. Sum prep and cook time columns. Create SQL temp views for custom logic.
4. Write data to output folder in CSV format.

# Tools used
Apache Spark, Python (Scripts and Notebooks), PySpark, SQL, Docker, VSCode

