import numpy as np
import pandas as pd

import keras
import tensorflow as tf
from tensorflow.keras import models
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM, GRU, Conv1D,GlobalMaxPooling1D
from tensorflow.keras.layers import MaxPooling1D, GlobalAveragePooling1D
from tensorflow.keras.layers import Flatten, Dropout, Input, Concatenate, Bidirectional

import ast

from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder.getOrCreate()

def init_model():
    regressor = Sequential()
    regressor.add(LSTM(units=50, return_sequences=True, input_shape=(15, 1)))
    regressor.add(Dropout(0.2))
    regressor.add(LSTM(units=50, return_sequences=True))
    regressor.add(Dropout(0.2))
    regressor.add(LSTM(units=50, return_sequences=True))
    regressor.add(Dropout(0.2))
    regressor.add(LSTM(units=50))
    regressor.add(Dropout(0.2))
    regressor.add(Dense(units=1))
    optimizer = keras.optimizers.Adam(learning_rate=0.0001)
    regressor.compile(optimizer=optimizer, loss='mean_squared_error')
    return regressor

def predict(df):
    # df = df.copy()
    model = init_model()
    model.load_weights("./model/subway_lstm_500.h5")

    # X =np.array(df['features'].apply(ast.literal_eval).tolist())
    X = np.array(df['features'].tolist())
    
    print(X)
    
    y_pre = model.predict(X)
    df['predicted'] = y_pre
    df.drop(columns=['features'], inplace=True)

    spark_df = spark.createDataFrame(df)
    return spark_df