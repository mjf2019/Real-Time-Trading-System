from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
import json
import pandas as pd
import numpy as np
from pyspark.sql.functions import to_json
import ast
from influxdb_client import InfluxDBClient, Point, WritePrecision


def send_to_influxdb(measurement, values):
    # Define InfluxDB connection details
    token = "Uo7KqzgyntDSl0MqD57vDkSjO9FMf87oPB1z1tKc6keMC9RHyvux8XqEK_CjUh4GRCo-i2wJ7eCTiEn_YgPD5g=="
    org = "iust"
    bucket = "finantial"
    url = "http://localhost:8086"


    # Convert DataFrame to Pandas DataFrame
    pandas_df = values.toPandas()

    # Write data to InfluxDB
    for index, row in pandas_df.iterrows():
        point = Point("measurement_name").tag("key", row["key"]).field("value", row["value"])
        write_api.write(bucket=bucket, org=org, record=point)

    # Create InfluxDB client
    client = InfluxDBClient(url=url, token=token, org=org)
    # Write data to InfluxDB
    write_api = client.write_api()
    point = Point(measurement).field("rsi_signal", values["rsi_signal"]) \
                                .field("rsi", values["rsi"]) \
                                .field("ms_signal", values["ms_signal"]) \
                                .field("mean_square", values["mean_square"]) \
                                .field("ems_signal", values["ems_signal"]) \
                                .field("ems_prev", values["ems_prev"]) \
                                .time(WritePrecision.NS)
    write_api.write(bucket=bucket, record=point)


def mean_square_func(data):
    return np.mean(np.square(data))

def exponential_mean_square(data, alpha):
    ems = np.zeros_like(data, dtype=float)
    ems[0] = data[0] ** 2
    for i in range(1, len(data)):
        ems[i] = alpha * ems[i - 1] + (1 - alpha) * data[i] ** 2
    return ems

def relative_strength_index(data, window=14):
    close_price = np.array(data)
    daily_returns = np.diff(close_price)

    # Calculate average gain and average loss
    gains = np.where(daily_returns > 0, daily_returns, 0)
    losses = np.where(daily_returns < 0, -daily_returns, 0)

    avg_gain = np.mean(gains[:window])
    avg_loss = np.mean(losses[:window])

    rs = avg_gain / (avg_loss + 1e-10)
    rsi = 100 - (100 / (1 + rs))
    return rsi
    
# Function for select last n values
def select_last_14(data_dict):

    #print(data_dict)
    for key, prices in data_dict.items():
        if (len(prices) >= 14):
            data_dict[key] = data_dict[key][-14:]
        else:
            diff = 14 - len(prices)
            i = 0
            while i < diff:
                data_dict[key].append(0)
                i += 1
    return data_dict

def calculate_signals(data, period=14, rsi_threshold=55, ms_threshold=0.1, ems_threshold=0.1):

    # Threshold for Mean Square (example: 2 times the standard deviation)
    ms_threshold = 2 * np.std(np.diff(data))


    alpha_value = 0.1
    mean_square = mean_square_func(data)
    ems = exponential_mean_square(data, alpha_value)
    rsi = relative_strength_index(data)
    
    # Threshold for Exponential Mean Square
    ems_threshold = np.mean(ems)
    print("")
    print(ms_threshold)
    print(ems_threshold)
    print(rsi_threshold)
    print("")
    # Define signals based on thresholds
    rsi_signal = 'Buy' if rsi< rsi_threshold else 'Sell'
    ms_signal = 'Buy' if mean_square < ms_threshold else 'Sell'
    ems_signal = 'Buy' if ems[-1] < ems_threshold else 'Sell'

        # Sample values for each metric
    values = [
        ("rsi_signal", rsi_signal),
        ("rsi", rsi),
        ("ms_signal", ms_signal),
        ("mean_square", mean_square),
        ("ems_signal", ems_signal),
        ("ems", ems[-1])
    ]
    spark = SparkSession.builder \
        .appName("InfluxDB Data Sender") \
        .getOrCreate()
    df = spark.createDataFrame(values, ["key", "value"])

    # Send all values together to InfluxDB
    send_to_influxdb("signal", values)
    return rsi_signal, rsi, ms_signal, mean_square, ems_signal, ems[-1]


# Function to update the dictionary with new data
def update_dict(rdd):

    rdd_list = rdd.collect()

    # Dictionary to store data
    data_dict = {}
    for input_data in rdd_list:

        rdd_dict = ast.literal_eval(input_data)

        name = rdd_dict['stock_symbol']
        
        if name not in data_dict:
            data_dict[name] = []
        
        price = rdd_dict['closing_price']
        data_dict[name].append(float(price))
                
    data_dict = select_last_14(data_dict)
    # Calculate and print signals for each key in the dictionary
    for key, prices in data_dict.items():
        rsi_signal, rsi, ms_signal, mean_square, ems_signal, ems = calculate_signals(prices)
        print(f"Signals for {key}:")
        print(f"RSI Value: {rsi}")
        print(f"RSI Signal: {rsi_signal}")
        print(f"Mean Square Value: {mean_square}")
        print(f"Mean Square Signal: {ms_signal}")
        print(f"Exponential Mean Square Value: {ems}")
        print(f"Exponential Mean Square Signal: {ems_signal}")
        print("\n")

    
if __name__ == "__main__":


    sc = SparkContext("local[2]", "SocketStreamExample")
    # set batch interval to 1 second (minimum value)
    ssc = StreamingContext(sc, 1)

    # Create a DStream that will connect to a socket and receive raw binary data
    samples = ssc.socketTextStream("localhost", 9999)

    #json_data_stream = samples.map(lambda line: json.loads(line))
    #print(json_data_stream)

    # Update dictionary with new data
    # samples.foreachRDD(lambda rdd: rdd.foreach(update_dict))
    samples.foreachRDD(update_dict)

    # Process each sample
    #samples.foreachRDD(lambda rdd: rdd.foreach(process_sample))

    # Start the streaming computation
    ssc.start()

    # Keep the program running
    try:
        # Sleep for a long time to keep the Spark Streaming context running
        # You can interrupt the program when you want to stop it
        ssc.awaitTermination()  # Timeout set to 24 hours (in seconds)
    except KeyboardInterrupt:
        # Stop the Spark Streaming context if interrupted
        ssc.stop()
