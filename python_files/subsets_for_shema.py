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




# Get all the tables based on the schema
# Film information tables
film_info = df_film[['title','year','genre', 'duration','rating']]
make_checkpoint()
crew_cast = df_film[['title','director','cast', 'distributor']]
make_checkpoint()
income = df_film[['title','domestic_k','international_k', 'worldwide_k', 'dom_pct', 'int_pct']]
make_checkpoint()
ranking = df_film[['title','place','duration_rank', 'income_rank', 'rank_diff']]
make_checkpoint()


# Genre tables
drama = pd.DataFrame(df_film[df_film['genre'] == 'Drama'][['title', 'place_bygenre', 'duration_rank_bygenre', 'income_rank_bygenre']].reset_index(drop = True))
make_checkpoint()
action = pd.DataFrame(df_film[df_film['genre'] == 'Action'][['title', 'place_bygenre', 'duration_rank_bygenre', 'income_rank_bygenre']].reset_index(drop = True))
make_checkpoint()
adventure = pd.DataFrame(df_film[df_film['genre'] == 'Adventure'][['title', 'place_bygenre', 'duration_rank_bygenre', 'income_rank_bygenre']].reset_index(drop = True))
make_checkpoint()
crime = pd.DataFrame(df_film[df_film['genre'] == 'Crime'][['title', 'place_bygenre', 'duration_rank_bygenre', 'income_rank_bygenre']].reset_index(drop = True))
make_checkpoint()
comedy = pd.DataFrame(df_film[df_film['genre'] == 'Comedy'][['title', 'place_bygenre', 'duration_rank_bygenre', 'income_rank_bygenre']].reset_index(drop = True))
make_checkpoint()
biography = pd.DataFrame(df_film[df_film['genre'] == 'Biography'][['title', 'place_bygenre', 'duration_rank_bygenre', 'income_rank_bygenre']].reset_index(drop = True))
make_checkpoint()

Animation = pd.DataFrame(df_film[df_film['genre'] == 'Animation'][['title', 'place_bygenre', 'duration_rank_bygenre', 'income_rank_bygenre']].reset_index(drop = True))
Horror = pd.DataFrame(df_film[df_film['genre'] == 'Horror'][['title', 'place_bygenre', 'duration_rank_bygenre', 'income_rank_bygenre']].reset_index(drop = True))
Mystery = pd.DataFrame(df_film[df_film['genre'] == 'Mystery'][['title', 'place_bygenre', 'duration_rank_bygenre', 'income_rank_bygenre']].reset_index(drop = True))
Western = pd.DataFrame(df_film[df_film['genre'] == 'Western'][['title', 'place_bygenre', 'duration_rank_bygenre', 'income_rank_bygenre']].reset_index(drop = True))
Film_Noir = pd.DataFrame(df_film[df_film['genre'] == 'Film-Noir'][['title', 'place_bygenre', 'duration_rank_bygenre', 'income_rank_bygenre']].reset_index(drop = True))
frames = [Animation, Horror, Mystery, Western, Film_Noir]
other_genre = pd.concat(frames).reset_index(drop = True)
make_checkpoint()





# Convert all tables into spark data frame
df_film_info_spark = spark.createDataFrame(film_info)
df_crew_cast_spark = spark.createDataFrame(crew_cast)
df_income_spark = spark.createDataFrame(income)
df_ranking_spark = spark.createDataFrame(ranking)
df_drama_spark = spark.createDataFrame(drama)
df_action_spark = spark.createDataFrame(action)
df_adventure_spark = spark.createDataFrame(adventure)
df_crime_spark = spark.createDataFrame(crime)
df_comedy_spark = spark.createDataFrame(comedy)
df_biography_spark = spark.createDataFrame(biography)
df_other_genre_spark = spark.createDataFrame(other_genre)



df_film_info_spark.printSchema()
df_crew_cast_spark.printSchema()
df_income_spark.printSchema()
df_ranking_spark.printSchema()
df_drama_spark.printSchema()
df_action_spark.printSchema()
df_adventure_spark.printSchema()
df_crime_spark.printSchema()
df_comedy_spark.printSchema()
df_biography_spark.printSchema()
df_other_genre_spark.printSchema()


# Convert all data frames into parquet files
df_film_info_spark.write.parquet("/project/Individual/parquet_files/film_info.parquet", mode = 'overwrite')
df_crew_cast_spark.write.parquet("/project/Individual/parquet_files/crew_cast.parquet", mode = 'overwrite')
df_income_spark.write.parquet("/project/Individual/parquet_files/income.parquet", mode = 'overwrite')
df_ranking_spark.write.parquet("/project/Individual/parquet_files/ranking.parquet", mode = 'overwrite')
df_drama_spark.write.parquet("/project/Individual/parquet_files/drama.parquet", mode = 'overwrite')
df_action_spark.write.parquet("/project/Individual/parquet_files/action.parquet", mode = 'overwrite')
df_adventure_spark.write.parquet("/project/Individual/parquet_files/adventure.parquet", mode = 'overwrite')
df_crime_spark.write.parquet("/project/Individual/parquet_files/crime.parquet", mode = 'overwrite')
df_comedy_spark.write.parquet("/project/Individual/parquet_files/comedy.parquet", mode = 'overwrite')
df_biography_spark.write.parquet("/project/Individual/parquet_files/biography.parquet", mode = 'overwrite')
df_other_genre_spark.write.parquet("/project/Individual/parquet_files/other_genre.parquet", mode = 'overwrite')
make_checkpoint()


