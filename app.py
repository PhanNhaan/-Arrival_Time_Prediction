from flask import Flask, render_template, jsonify
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
import base64
from flask import Flask, render_template, jsonify
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
import base64
import time
from pymongo import MongoClient

app = Flask(__name__)

client = MongoClient("localhost:27017")
collection = client.arrive_time.subway

def generate_plot(actual_prices, predicted_prices):
    plt.figure(figsize=(10, 6))
    plt.plot(actual_prices, marker='o', linestyle='-', color='b', label='Actual Time')
    plt.plot(predicted_prices, marker='o', linestyle='-', color='r', label='Predicted Time')
    plt.title('Delay Time Prediction')
    plt.xlabel('Trip Number')
    plt.ylabel('Delay Time')
    plt.grid(True)
    plt.legend()

    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    
    image_png = buffer.getvalue()
    buffer.close()
    graph = base64.b64encode(image_png).decode('utf-8')

    return graph

@app.route('/')
def index():
    return render_template('index.html')

# @app.route('/update_prediction')
# def update_prediction():
#     predicted_price = db['stocks'].find_one()['predicted_price']
#     return jsonify({'predicted_price': predicted_price})

@app.route('/data')
def get_data():
    # Lấy dữ liệu từ MongoDB
    data = list(collection.find())
    for item in data:
        item['_id'] = str(item['_id'])
    return jsonify(data)

@app.route('/update_plot')
def update_plot():
    stock_data = [i for i in collection.find()]
    predicted_prices = [item["predicted"] for item in stock_data]
    actual_prices = [item["min_delay"] for item in stock_data]

    graph = generate_plot(actual_prices, predicted_prices)
    return graph

if __name__ == '__main__':
    app.run(debug=True)