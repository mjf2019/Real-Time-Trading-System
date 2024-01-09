from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Function to process each sample
def process_sample(sample):
    # Your processing logic here
    # This example simply prints the sample
    print(f"Processing sample: {sample}")
    print("")

# Create a local StreamingContext with two working threads and a batch interval of 1 second
sc = SparkContext("local[2]", "SocketStreamExample")
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to a socket and receive raw binary data
samples = ssc.socketTextStream("localhost", 9999)

# Process each sample
samples.foreachRDD(lambda rdd: rdd.foreach(process_sample))
print("")
print("---------------------------------------------------")
print("---------------------------------------------------")
print("")
# Start the streaming computation
ssc.start()

# Keep the program running
try:
    # Sleep for a long time to keep the Spark Streaming context running
    # You can interrupt the program when you want to stop it
    ssc.awaitTerminationOrTimeout(86400)  # Timeout set to 24 hours (in seconds)
except KeyboardInterrupt:
    # Stop the Spark Streaming context if interrupted
    ssc.stop()
