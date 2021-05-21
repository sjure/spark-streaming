from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from process import process_rdd

conf = SparkConf()
conf.setAppName("NewsStreaming")

sc = SparkContext("local[2]", "NetworkWordCount",conf=conf)
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream("localhost", 9000)
words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.foreachRDD(process_rdd)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
