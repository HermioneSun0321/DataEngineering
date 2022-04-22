#!/usr/bin/env python
# coding: utf-8


# Import libraries
import pandas as pd
import numpy as np
import os

from dvc.api import make_checkpoint

from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"
import warnings
warnings.filterwarnings("ignore")


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"



# import pyspark 
from pyspark.sql import SparkSession
spark = SparkSession     .builder     .appName("PySpark App")     .config("spark.jars", "postgresql-42.3.2.jar")     .getOrCreate()



# Read film data
df_film = spark.read.parquet("/project/Individual/parquet_files/origin.parquet").toPandas()
make_checkpoint()


# A glance at the duration column
df_film['duration']

# Change the format of unique running times
# Such as less than 1 hour (no 'hr') or exactly whole hours (no 'min')
less_1hr = df_film[df_film['duration'].str.contains('hr')== False]
less_1hr

whole_hrs = df_film[df_film['duration'].str.contains('min')== False]
whole_hrs

# Mannually fix the format
df_film.at[4, 'duration'] = '0 hr 45 min'
df_film.at[16, 'duration'] = '2 hr 0 min'
df_film.at[24, 'duration'] = '2 hr 0 min'
df_film.at[25, 'duration'] = '2 hr 0 min'
df_film.at[72, 'duration'] = '2 hr 0 min'
df_film.at[134, 'duration'] = '3 hr 0 min'
make_checkpoint()


# Clean the duration column for transformation
df_film['duration'] = df_film['duration'].str.replace(' ', '')
df_film['duration'] = df_film['duration'].str.replace('min', '')
df_film['duration'] = df_film['duration'].str.replace('hr', ':')
make_checkpoint()


# Convert duration into minutes
df_film['duration'] = df_film['duration'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1]))
make_checkpoint()


# Create duration rank column
df_film['duration_rank'] = df_film['duration'].rank(ascending=False)
make_checkpoint()


# Create duration by genre column
df_film['duration_rank_bygenre'] = df_film.groupby('genre')['duration'].rank(ascending=False)
make_checkpoint()



# Calculate the percentage of domestic and international box office income
df_film['dom_pct'] = df_film['domestic_k']/df_film['worldwide_k']
df_film['int_pct'] = df_film['international_k']/df_film['worldwide_k']
df_film['dom_pct'] = df_film['dom_pct'].round(decimals = 4)
df_film['int_pct'] = df_film['int_pct'].round(decimals = 4)
make_checkpoint()


# Check the realistic feasibility
df_film['check'] = df_film['dom_pct'] + df_film['int_pct']
df_film['check'] = df_film['check'].fillna(0)
make_checkpoint()

# Find the rows that do not make sense in real life
df_film.loc[(df_film['check'] != 1.0000) & (df_film['check'] != 0)]


# Mannually fix the films that do not have international incomes
df_film.at[1, 'international_k'] = 0.000
df_film.at[1, 'worldwide_k'] = 144.738
df_film.at[18, 'international_k'] = 0.000
df_film.at[18, 'worldwide_k'] = 21.877
df_film.at[27, 'international_k'] = 0.000
df_film.at[27, 'worldwide_k'] = 52287.414
df_film.at[58, 'international_k'] = 0.000
df_film.at[58, 'worldwide_k'] = 52767.889
df_film.at[76, 'international_k'] = 0.000
df_film.at[76, 'worldwide_k'] = 11487.676
df_film.at[104, 'international_k'] = 0.000
df_film.at[104, 'worldwide_k'] = 46357.676
df_film.at[160, 'international_k'] = 0.000
df_film.at[160, 'worldwide_k'] = 3753.929
df_film.at[164, 'international_k'] = 0.000
df_film.at[164, 'worldwide_k'] = 27200.000
df_film.at[179, 'international_k'] = 0.000
df_film.at[179, 'worldwide_k'] = 933.933
df_film.at[226, 'international_k'] = 0.000
df_film.at[226, 'worldwide_k'] = 23341.568
df_film.at[235, 'international_k'] = 0.000
df_film.at[235, 'worldwide_k'] = 516.962
make_checkpoint()


# Find other missing box office incomes data
df_film.loc[(df_film['domestic_k'] == 0) & (df_film['international_k'] == 0) & (df_film['worldwide_k'] == 0)]


# Fill in missing values mannully
df_film.at[5, 'international_k'] = 1.098
df_film.at[5, 'worldwide_k'] = 1.098
df_film.at[7, 'international_k'] = 198.992
df_film.at[7, 'worldwide_k'] = 198.992
df_film.at[9, 'international_k'] = 286.085
df_film.at[9, 'worldwide_k'] = 286.085
df_film.at[17, 'international_k'] = 1740.429
df_film.at[17, 'worldwide_k'] = 1740.429
df_film.at[22, 'international_k'] = 40.468
df_film.at[22, 'worldwide_k'] = 40.468
df_film.at[42, 'international_k'] = 72.275
df_film.at[42, 'worldwide_k'] = 72.275
df_film.at[46, 'international_k'] = 46749.646
df_film.at[46, 'worldwide_k'] = 46749.646
df_film.at[48, 'international_k'] = 14.480
df_film.at[48, 'worldwide_k'] = 14.480
df_film.at[49, 'international_k'] = 18612.999
df_film.at[49, 'worldwide_k'] = 18612.999
df_film.at[51, 'international_k'] = 90.556
df_film.at[51, 'worldwide_k'] = 90.556
df_film.at[64, 'international_k'] = 7.693
df_film.at[64, 'worldwide_k'] = 7.693
df_film.at[93, 'domestic_k'] = 46.808
df_film.at[93, 'worldwide_k'] = 46.808
df_film.at[97, 'domestic_k'] = 35.566
df_film.at[97, 'worldwide_k'] = 35.566
df_film.at[105, 'international_k'] = 14.190
df_film.at[105, 'worldwide_k'] = 14.190
df_film.at[110, 'domestic_k'] = 156000.000
df_film.at[110, 'worldwide_k'] = 156000.000
df_film.at[126, 'domestic_k'] = 15000.000
df_film.at[126, 'worldwide_k'] = 15000.000
df_film.at[129, 'international_k'] = 41.960
df_film.at[129, 'worldwide_k'] = 41.960
df_film.at[130, 'international_k'] = 195.088
df_film.at[130, 'worldwide_k'] = 195.088
df_film.at[135, 'international_k'] = 12.180
df_film.at[135, 'worldwide_k'] = 12.180
df_film.at[146, 'domestic_k'] = 5014.000
df_film.at[146, 'worldwide_k'] = 5014.000
df_film.at[147, 'domestic_k'] = 46.808
df_film.at[147, 'worldwide_k'] = 46.808
df_film.at[148, 'international_k'] = 228.178
df_film.at[148, 'worldwide_k'] = 228.178
df_film.at[174, 'international_k'] = 26.916
df_film.at[174, 'worldwide_k'] = 26.916
df_film.at[183, 'international_k'] = 14.524
df_film.at[183, 'worldwide_k'] = 14.524
df_film.at[192, 'international_k'] = 0.955
df_film.at[192, 'worldwide_k'] = 0.955
df_film.at[234, 'international_k'] = 15.222
df_film.at[234, 'worldwide_k'] = 15.222
df_film.at[247, 'international_k'] = 5.252
df_film.at[247, 'worldwide_k'] = 5.252
df_film.at[249, 'international_k'] = 970.214
df_film.at[249, 'worldwide_k'] = 970.214
make_checkpoint()


# Check again
df_film['dom_pct'] = df_film['domestic_k']/df_film['worldwide_k']
df_film['int_pct'] = df_film['international_k']/df_film['worldwide_k']
df_film['dom_pct'] = df_film['dom_pct'].round(decimals = 4)
df_film['int_pct'] = df_film['int_pct'].round(decimals = 4)
df_film['check'] = df_film['dom_pct'] + df_film['int_pct']
df_film['check'] = df_film['check'].fillna(0)
make_checkpoint()
df_film['check'].value_counts()


# Drop the check column
df_film = df_film.drop('check', axis = 1)
make_checkpoint()



# Create place by genre column
df_film['place_bygenre'] = df_film.groupby('genre')['place'].rank(ascending=False)
make_checkpoint()


# Create income rank column
df_film['income_rank'] = df_film['worldwide_k'].rank(ascending=False)
make_checkpoint()


# Create income rank by genre column
df_film['income_rank_bygenre'] = df_film.groupby('genre')['worldwide_k'].rank(ascending=False)
make_checkpoint()


# Calculate the difference between rating rank (place) and income rank
df_film['rank_diff'] = df_film['income_rank'] - df_film['place']
make_checkpoint()



# Check for missing values
df_film.isnull().sum()

# Fill in NaN with 0
df_film.fillna(0, inplace=True)
make_checkpoint()

# Check again
df_film.isnull().sum()



# Adjust the order of the columns
df_film = df_film[['title', 'year', 'director', 'cast', 'distributor', 'genre', 'rating', 'place', 'place_bygenre', 'duration', 'duration_rank', 'duration_rank_bygenre', 'domestic_k', 'international_k', 'worldwide_k', 'dom_pct', 'int_pct', 'income_rank', 'income_rank_bygenre', 'rank_diff']]
make_checkpoint()



# Convert the data frame into spark data frame
df_film_spark = spark.createDataFrame(df_film)


df_film_spark.printSchema()


# Convert the data frame into parquet format
df_film_spark.write.parquet("/project/Individual/parquet_files/film.parquet", mode = 'overwrite')
make_checkpoint()