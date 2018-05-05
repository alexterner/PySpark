import sys

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.streaming import StreamingContext
except ImportError:
    SparkContext = None
    SparkConf = None
    StreamingContext = None
    print("Error importing Spark Modules")
    sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: StreamDemoSockets <hostname> <port>"
        exit(-1)

    cnf = SparkConf()
    cnf.set("spark.eventLog.enabled", "True")
    cnf.set("spark.eventLog.dir", "file:////tmp/spark_logs")
    cnf.setAppName("StreamDemoSockets")
    cnf.setMaster("local[*]")

    # 2 stands for the number of seconds
    sc = SparkContext(conf=cnf)
    ssc = StreamingContext(sc, 1)
    sc.setLogLevel("ERROR")

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    words = lines.flatMap(lambda line: line.split(" "))

    initWordCount = words.map(lambda word: (word, 1))

    # Reduce last 30 seconds of data, every 10 seconds
    refCounts = initWordCount.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)

    # transform - Return a new DStream by applying a RDD-to-RDD function to every RDD of the source DStream.
    # This can be used to do arbitrary RDD operations on the DStream.
    sortedstream = refCounts.transform(
        lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

    ssc.checkpoint("/tmp/checkpoint/")
    sortedstream.pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
