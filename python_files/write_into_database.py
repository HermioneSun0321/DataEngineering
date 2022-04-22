#!/usr/bin/env python
# coding: utf-8



import os

from dvc.api import make_checkpoint

from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"
import warnings
warnings.filterwarnings("ignore")


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"



from pyspark.sql import SparkSession
spark = SparkSession     .builder     .appName("PySpark App")     .config("spark.jars", "postgresql-42.3.2.jar")     .getOrCreate()



# Create the schema
!PGPASSWORD=qwerty123 psql -h depgdb.crhso94tou3n.eu-west-2.rds.amazonaws.com -d yihansun21 -U yihansun21 -c '\i schema.sql'


# Read all the tables
film_info = spark.read.parquet("/project/Individual/parquet_files/film_info.parquet")
crew_cast = spark.read.parquet("/project/Individual/parquet_files/crew_cast.parquet")
income = spark.read.parquet("/project/Individual/parquet_files/income.parquet")
ranking = spark.read.parquet("/project/Individual/parquet_files/ranking.parquet")
drama = spark.read.parquet("/project/Individual/parquet_files/drama.parquet")
action = spark.read.parquet("/project/Individual/parquet_files/action.parquet")
adventure = spark.read.parquet("/project/Individual/parquet_files/adventure.parquet")
crime = spark.read.parquet("/project/Individual/parquet_files/crime.parquet")
comedy = spark.read.parquet("/project/Individual/parquet_files/comedy.parquet")
biography = spark.read.parquet("/project/Individual/parquet_files/biography.parquet")
other_genre = spark.read.parquet("/project/Individual/parquet_files/other_genre.parquet")
make_checkpoint()


# Information for postgresql login
postgres_uri = "jdbc:postgresql://depgdb.crhso94tou3n.eu-west-2.rds.amazonaws.com:5432/yihansun21"
user = "yihansun21"
password = "qwerty123"


# Write the tables into database
film_info.write.jdbc(url=postgres_uri, table="films.film_info", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
crew_cast.write.jdbc(url=postgres_uri, table="films.crew_cast", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
income.write.jdbc(url=postgres_uri, table="films.income", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
ranking.write.jdbc(url=postgres_uri, table="films.ranking", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
drama.write.jdbc(url=postgres_uri, table="films.drama", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
action.write.jdbc(url=postgres_uri, table="films.action", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
adventure.write.jdbc(url=postgres_uri, table="films.adventure", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
crime.write.jdbc(url=postgres_uri, table="films.crime", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
comedy.write.jdbc(url=postgres_uri, table="films.comedy", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
biography.write.jdbc(url=postgres_uri, table="films.biography", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()
other_genre.write.jdbc(url=postgres_uri, table="films.other_genre", mode="append", properties={"user":user, "password": password, "driver": "org.postgresql.Driver" })
make_checkpoint()

