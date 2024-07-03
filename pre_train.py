import numpy as np
import pandas as pd

import re
from string import punctuation

import pandas as pd
import numpy as np

from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler

from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences


subway = pd.read_csv('./data/clean_data/subway_cleaned.csv')

indexed_data = subway.copy()

# 
le = LabelEncoder()
le_day = le.fit(indexed_data["day"].unique())
indexed_data["day_indexed"] = le_day.transform(indexed_data["day"])

le = LabelEncoder()
le_code = le.fit(indexed_data["code"].unique())
indexed_data["code_indexed"] = le_code.transform(indexed_data["code"])

le = LabelEncoder()
le_bound = le.fit(indexed_data["bound"].unique())
indexed_data["bound_indexed"] = le_bound.transform(indexed_data["bound"])

le = LabelEncoder()
le_line = le.fit(indexed_data["line"].unique())
indexed_data["line_indexed"] = le_line.transform(indexed_data["line"])

# 
tokenizer = Tokenizer(num_words=10000)
tokenizer.fit_on_texts(indexed_data['station']) 
sequences = tokenizer.texts_to_sequences(indexed_data['station'])
station_w2v = pad_sequences(sequences, maxlen=4) 
indexed_data['station_w2v'] = station_w2v.tolist()

# 
feature_cols =['min_gap',
 'vehicle',
 'day_month',
 'month',
 'hour',
 'min',
 'at_station',
 'day_indexed',
 'code_indexed',
 'bound_indexed',
 'line_indexed'
 ]
features_data = indexed_data.copy()
selected_columns_df = features_data[feature_cols].values

features_data['features'] = features_data['station_w2v']
for i in range(len(selected_columns_df)):
    features_data['features'][i] = features_data['features'][i] + selected_columns_df[i].tolist()

# 
array = np.array(features_data['features'].tolist())

scaler = MinMaxScaler()
scaler_model =scaler.fit(array)

scaled_array = scaler_model.transform(array)
features_data['scaled'] = scaled_array.tolist()


def clean_station_col(text):
    text = re.sub(rf'[{punctuation}]', '',text) # Remove punctuation
    text = re.sub('\s+(BD|SRT|YUS|YU)+\s',' ',text) # Remove BD, SRT,YU,YUS
    text = re.sub('CTR','CENTRE',text) # Replace ctr with centre
    text = re.sub('(SATATIO|STAITON|STATI|CAR HOUSE|STN|STATIO|YARD|WYE|HOSTLER|CARHOUSE|SHOP|SHOPS|LOWER|COMMERCE|HOSTLE)+$',' STATION',text) # Replace texts not end with station to station
    # split by (, TO, AND and only the words before them
    text = text.split('(')[0]
    text = text.split(' TO ')[0]
    text = text.split(' AND ')[0]
    if 'YONGEUNIVERSITY' in text or 'YONGE UNIVERSITY' in text:
        text = 'YONGE UNIVERSITY LINE'
    if  not text.endswith('LINE') and not text.endswith('SUBWAY') and 'STATION' not in text:
        text = text + ' STATION'
    if 'SCARB' in text:
        text = 'SCARBOROUGH CENTRE STATION'
    if 'DAVISVILLE' in text:
        text = 'DAVISVILLE STATION'
    if 'GREENWOOD' in text:
        text = 'GREENWOOD STATION'
    if 'KEELE' in text:
        text = 'KEELE STATION'
    if 'MCCOWAN' in text:
        text = 'MCCOWAN STATION'
    if 'WILSON' in text:
        text = 'WILSON STATION'
    return text

def pre_process(X):
    X = X.copy()
    X['day_month'] =pd.to_datetime(X['date']).day
    X['month'] =pd.to_datetime(X['date']).month

    X['hour'] = pd.to_datetime(X['time']).hour
    X['min'] = pd.to_datetime(X['time']).minute

    X['station'] = clean_station_col(X['station'])
    X['station'] = X['station'].lower()
    X['at_station'] = 1 if 'station' in X['station'] else 0

    X['bound'] = 'unknown' if X['bound'] not in ['N','S','E','W'] else X['bound']

    X['line'] = 'YU/BD' if X['line'] in ['YU / BD', 'YUS/BD', 'BD/YU', 'YU & BD'] else X['line']
    X['line'] = 'YU' if X['line']== 'YUS' else X['line']
    X['line'] = 'BD' if X['line']== 'BD LINE 2' else X['line']

    return X

def feature(X):
    X = X.copy()
    X['day_indexed'] = le_day.transform([X['day']])[0]
    X['code_indexed'] = le_code.transform([X['code']])[0]
    X['bound_indexed'] = le_bound.transform([X['bound']])[0]
    X['line_indexed'] = le_line.transform([X['line']])[0]

    X['station_w2v'] = tokenizer.texts_to_sequences([X['station']])[0]
    X['station_w2v'] = pad_sequences([X['station_w2v']], maxlen=4)[0]

    selected_columns = [X['min_gap'], X['vehicle'], X['day_month'], X['month'], X['hour'], X['min'], X['at_station'], X['day_indexed'], X['code_indexed'], X['bound_indexed'], X['line_indexed']]
    X['features'] = X['station_w2v'].tolist() + selected_columns
    X['features'] = scaler_model.transform([X['features']])[0]
    return X


