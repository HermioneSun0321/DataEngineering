#!/usr/bin/env python
# coding: utf-8


# Import libraries
import pandas as pd
import numpy as np
import os
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import metrics
from flask import Flask, request, jsonify
import joblib
import traceback
import json

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



# Build a machine learning to predict success of worldwide box office income
# New data frame for machine learning model
df_ml = df_film

# Keep only needed columns
df_ml = df_ml[['title', 'year', 'distributor', 'genre', 'rating', 'duration', 'worldwide_k']]
make_checkpoint()




# Create dummy varaibles for distributor
# Only identify if the company is one of the top 10 film distributors
# Top 10 are: Warner Bros., Walt Disney Studios Motion Pictures, United Artists, Paramount Pictures, Universal Pictures, Columbia Pictures, Twentieth Century Fox, Miramax, Metro-Goldwyn-Mayer (MGM) and Sony Pictures Classics
df_ml['dum_distributor'] = 0
Top_10 = ['Warner Bros.', 'Walt Disney Studios Motion Pictures', 'United Artists', 'Paramount Pictures', 'Universal Pictures', 'Columbia Pictures', 'Twentieth Century Fox', 'Miramax', 'Metro-Goldwyn-Mayer (MGM)', 'Sony Pictures Classics']
for i in df_ml.index:
    if df_ml.at[i,'distributor'] in Top_10:
        df_ml.at[i,'dum_distributor'] = 1
    else:
        df_ml.at[i,'dum_distributor'] = 0
make_checkpoint()




# Create dummy varaibles for genre
# The least 5 seen genres are identified as other genres
df_ml['dum_drama'] = np.where(df_ml['genre'] == 'Drama', 1, 0)
df_ml['dum_action'] = np.where(df_ml['genre'] == 'Action', 1, 0)
df_ml['dum_adventure'] = np.where(df_ml['genre'] == 'Adventure', 1, 0)
df_ml['dum_crime'] = np.where(df_ml['genre'] == 'Crime', 1, 0)
df_ml['dum_comedy'] = np.where(df_ml['genre'] == 'Comedy', 1, 0)
df_ml['dum_biography'] = np.where(df_ml['genre'] == 'Biography', 1, 0)

list_othergenre = ['Animation', 'Horror', 'Mystery', 'Western', 'Film-Noir']
df_ml['dum_othergenre'] = 0
for i in df_ml.index:
    if df_ml.at[i,'genre'] in list_othergenre:
        df_ml.at[i,'dum_othergenre'] = 1
    else:
        df_ml.at[i,'dum_othergenre'] = 0
make_checkpoint()





# Create the y variable (whether the film succeed in box office income)
# success = 1 if worldwide income > median of all 250 films, = 0 otherwise
median_income = df_ml['worldwide_k'].median()

df_ml['success'] = 0
for i in df_ml.index:
    if df_ml.at[i,'worldwide_k'] > median_income:
        df_ml.at[i,'success'] = 1
    else:
        df_ml.at[i,'success'] = 0
make_checkpoint()




# Drop origianl distributor and genre columns
df_ml = df_ml.drop(['distributor', 'genre', 'worldwide_k'],axis =1)
make_checkpoint()

# Adjust the order of the columns
df_ml = df_ml[['title', 'year', 'rating', 'duration', 'dum_distributor', 'dum_drama', 'dum_action', 'dum_adventure', 'dum_crime', 'dum_comedy', 'dum_biography', 'dum_othergenre', 'success']]
make_checkpoint()




# Convert the data frame into spark data frame
df_ml_spark = spark.createDataFrame(df_ml)

df_ml_spark.printSchema()

# Convert the data frame into parquet format
df_ml_spark.write.parquet("/project/Individual/parquet_files/ml.parquet", mode = 'overwrite')
make_checkpoint()




# Train a logistic regression model for prediction

# Split variables and train/test sets
X = df_ml.drop(['title', 'success'], axis = 1)
y = df_ml['success']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

mlr = LogisticRegression()
mlr.fit(X_train, y_train)

#Predict the response for test dataset
y_pred_train = mlr.predict(X_train)
y_pred_test = mlr.predict(X_test)

# Model Accuracy
acc_train = metrics.accuracy_score(y_train, y_pred_train)
acc_test = metrics.accuracy_score(y_test, y_pred_test)
print(f"Accuracy in train set: {acc_train:.3f}")
print(f"Accuracy in test set: {acc_test:.3f}")



#Saving the model
joblib.dump(mlr, 'model.pkl')
lr = joblib.load('model.pkl')

model_columns = list(X.columns)
joblib.dump(model_columns, 'model_columns.pkl')


# API definition using Flask
app = Flask(__name__)

@app.route('/predict', methods=['POST'])
def predict():
    if lr:
        try:
            json_ = request.json
            print(json_)
            query = pd.get_dummies(pd.DataFrame(json_))
            query = query.reindex(columns=model_columns, fill_value=0)

            prediction = list(lr.predict(query))

            return jsonify({'prediction': str(prediction)})

        except:

            return jsonify({'trace': traceback.format_exc()})
    else:
        print ('Train the model first')
        return ('No model here to use')

if __name__ == '__main__':
    try:
        port = int(sys.argv[1]) # This is for a command-line input
    except:
        port = 12345

    lr = joblib.load("model.pkl")
    model_columns = joblib.load("model_columns.pkl")


    app.run(debug=True, use_reloader=False)
    
