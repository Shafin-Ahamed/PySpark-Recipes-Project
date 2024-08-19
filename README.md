# Overview

For this project, I performed an ETL of cooking recipes using  Python, SQL, and a docker container I found from: https://github.com/jplane/pyspark-devcontainer

# Tools used
Python (Scripts and Notebooks), SQL, Docker, VSCode, Git

# Approach
1. I extracted data from JSON files into PySpark dataframes
2. Transformations: Add columns to convert ISO 8601 duration into minutes. Sum cook and prep time columns. Create SQL temp views for custom logic to aggregate recipes based on difficulties.
3. Write data to output folder in CSV format

# Special Thanks
1. https://github.com/jplane/pyspark-devcontainer  -> Github repository I originally cloned and used for my docker container
2. https://stackoverflow.com/questions/69735290/apache-spark-parse-pt2h5m-duration-iso-8601-duration-in-minutes  -> Helped me understand how to convert ISO 8601 columns
3. Youtube channel "CodewithAJN". Thank you to Ajay Negi for helping me understand how to create my own Python venv's for Spark!


