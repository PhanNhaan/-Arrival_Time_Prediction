from kafka import KafkaProducer
from json import dumps
from time import sleep
from random import seed
from random import randint
import pandas as pd
import datetime

import pre_train

topic_name = 'TimePredict'
kafka_server = 'localhost:9092'

df = pd.read_csv("./data/test.csv")
df.columns = [i.replace(' ', '_').lower() for i in df.columns]

producer = KafkaProducer(bootstrap_servers=kafka_server,value_serializer = lambda x:dumps(x).encode('utf-8'))

# sleep(20)
# df['time'] = pd.to_datetime(df['time'])
# df = df.sort_values(by="time",ascending=True)
# df['time'] = df['time'].dt.strftime("%Y-%m-%d")

seed(1)
    
for i in range(len(df)):
    X = pre_train.pre_process(df.loc[i])
    X = pre_train.feature(X)
    data = {
        "date": X['date'],
        "time": X["time"],
        "day": X["day"],
        "station": X["station"],
        "code": X["code"],
        "min_gap": int(X["min_gap"]),
        "bound": X["bound"],
        "line": X["line"],
        "vehicle": int(X["vehicle"]),
        "min_delay": int(X["min_delay"]),
        "features": X["features"].tolist(),
    }
    producer.send(topic_name, value=data)
    print(data)
    sleep(5)

producer.flush()