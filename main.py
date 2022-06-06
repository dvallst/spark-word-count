from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == "__main__":
    # Create a local SparkContext with 2 threads
    sc = SparkContext(master="local[2]", appName="WordCount")

    # Create a StreamingContext from the SparkContext with a batch duration of 2 second
    ssc = StreamingContext(sparkContext=sc, batchDuration=2)

    # Create a DStream that connects to "localhost" and port 9999 with a socket type connection (nc -lk 9999)
    lines = ssc.socketTextStream(hostname="localhost", port=9999)

    # Breaks each line into words
    words = lines.flatMap(lambda line: line.split(" "))

    # Transform "words" DStream to "tuples" DStream, with a value of 1 for each word
    tuples = words.map(lambda word: (word, 1))

    # Count the number of words in StateLess mode
    word_count = tuples.reduceByKey(lambda x, y: x + y)

    # Show the number of times each word appears in the RDD
    word_count.pprint()

    ssc.start()  # Start processing
    ssc.awaitTermination()  # Wait for the end of processing
