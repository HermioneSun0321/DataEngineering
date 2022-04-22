#!/usr/bin/env python
# coding: utf-8


# Import libraries
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import re
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


# Downloading imdb top 250 movie's data
url = 'http://www.imdb.com/chart/top'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'lxml')

movies = soup.select('td.titleColumn')
links = [a.attrs.get('href') for a in soup.select('td.titleColumn a')]
crew = [a.attrs.get('title') for a in soup.select('td.titleColumn a')]
ratings = [b.attrs.get('data-value') for b in soup.select('td.posterColumn span[name=ir]')]



# Create empty list for flim information
list_movie = []

# Iterating over movies to extract each movie's details
for index in range(0, len(movies)):

    movie_string = movies[index].get_text()
    movie = (' '.join(movie_string.split()).replace('.', ''))
    movie_title = movie[len(str(index))+1:-7]
    year = re.search('\((.*?)\)', movie_string).group(1)
    place = movie[:len(str(index))-(len(movie))]
    crew_names = crew[index].split(',')
    director = crew_names[0][:-7]
    star_cast = ','.join(crew_names[1:])
    
    data = {"movie_title": movie_title,
            "year": year,
            "place": place,
            "director":director,
            "star_cast": star_cast,
            "rating": ratings[index]}
    list_movie.append(data)



# Create empty lists for columns
list_place = []
list_title = []
list_year = []
list_director = []
list_cast = []
list_rating = []

# Get values of details from each flim
for movie in list_movie:
    list_place.append(movie['place'])
    list_title.append(movie['movie_title'])
    list_year.append(movie['year'])
    list_director.append(movie['director'])
    list_cast.append(movie['star_cast'])
    list_rating.append(movie['rating'])



# Create a dataframe from the lists
df = pd.DataFrame({'place':list_place, 'title':list_title, 'year':list_year, 'director':list_director, 'cast':list_cast, 'rating':list_rating})
# By mannually examing, the place of 10 and 100 are mistaken as 1 and 10
# Fix the mistake mannually
df.at[9, 'place'] = 10
df.at[99, 'place'] = 100
make_checkpoint()


# Get box office income and more information from IMDb Mojo website
# Creat empty lists for more information columns
list_domestic = []
list_international = []
list_worldwide = []
list_genre = []
list_duration = []
list_distributor = []

# Web scraping film detailed information from Box Office Mojo website of each film
for i in df.index:
    link = links[i]
    URL =f"https://www.boxofficemojo.com{link}?ref_=bo_se_r_1"
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    soup_body = str(soup.body)
    
    info = soup.findAll('div', attrs= {'class': 'a-section a-spacing-none mojo-gutter mojo-summary-table'})
    for x in info:
        earning = x.find_all('span', attrs = {'class': 'money'})
        if len(earning)>= 3:
            domestic = earning[0].text.replace('$', '').replace(',', '')
            list_domestic.append(domestic)
            international = earning[1].text.replace('$', '').replace(',', '')
            list_international.append(international)
            worldwide = earning[2].text.replace('$', '').replace(',', '')
            list_worldwide.append(worldwide)
        else: # Some films are missing one or more number on box office incomes
            list_domestic.append(0)
            list_international.append(0)
            list_worldwide.append(0)
    genre = re.findall(r'Genres</span><span>(.*)', soup_body)
    list_genre.append(genre[0])
    duration = re.findall(r'Running Time</span><span>(.*)</span></div><div class="a-section a-spacing-none"><span>Genres', soup_body)
    list_duration.append(duration[0])
    distributor = re.findall(r'Domestic Distributor</span><span>(.*)<br/>', soup_body)
    if len(distributor)>0:
        list_distributor.append(distributor[0])
    else: # Some filem are missing distributor information
        list_distributor.append(0)



# Add information to data frame
df['genre'] = list_genre
df['duration'] = list_duration
df['domestic'] = list_domestic
df['international'] = list_international
df['worldwide'] = list_worldwide
df['distributor'] = list_distributor
make_checkpoint()


# Clean the genre column
df['genre'].replace({'Drama</span></div><div class="a-section a-spacing-none"><span>': 'Drama'}, inplace=True)
df['genre'].replace({'Western</span></div><div class="a-section a-spacing-none"><span>': 'Western'}, inplace=True)
df['genre'].replace({'Horror</span></div><div class="a-section a-spacing-none"><span>': 'Horror'}, inplace=True)
df['genre'].replace({'Comedy</span></div><div class="a-section a-spacing-none"><span>': 'Comedy'}, inplace=True)
make_checkpoint()


# Chnage units of box office income into thousands
df[['domestic', 'international', 'worldwide']] = df[['domestic', 'international', 'worldwide']].astype(int)
df['domestic'] = df['domestic']/1000
df['international'] = df['international']/1000
df['worldwide'] = df['worldwide']/1000

# Renew column names
df = df.rename(columns = {'domestic':'domestic_k','international':'international_k', 'worldwide':'worldwide_k' })
make_checkpoint()


# Change column dtype before convert into spark data frame
df[['place', 'year']] = df[['place', 'year']].astype(int)
df[['rating']] = df[['rating']].astype(float)
df['rating'] = df['rating'].round(decimals = 4)
df[['title', 'director', 'cast', 'genre', 'duration','distributor']] = df[['title', 'director', 'cast', 'genre', 'duration', 'distributor']].astype(str)
make_checkpoint()


# Convert the data frame into spark data frame
df_origin_spark = spark.createDataFrame(df)


df_origin_spark.printSchema()


# Convert the data frame into parquet format
df_origin_spark.write.parquet("/project/Individual/parquet_files/origin.parquet", mode = 'overwrite')
make_checkpoint()

