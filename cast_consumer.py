from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Spark Stremaing setup
zk_server = "localhost:2181"
sc = SparkContext(appName="MovieDBAnalyser")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 600)  # batch interval of 10 mins

# Getting the ids of cast who were present in any of the movie release in Nov 2019
movieKafkaStream = KafkaUtils.createStream(
    ssc, zk_server, "MovieDBSparkStreaming", {"cast-movie": 1},
)
movies = (
    movieKafkaStream.map(lambda x: json.loads(x[1]))
    .map(lambda x: (x["cast"], 1))
    .reduceByKey(lambda x, y: x + y)
)

# Getting the ids of cast who were present in any of the tv shows aired in Nov 2019
tvKafkaStream = KafkaUtils.createStream(
    ssc, zk_server, "MovieDBSparkStreaming", {"cast-tv": 1},
)
tvShows = (
    tvKafkaStream.map(lambda x: json.loads(x[1]))
    .map(lambda x: (x["cast"], 1))
    .reduceByKey(lambda x, y: x + y)
)

# Joining the two streams based on key (cast id), the resulting stream will have cast ids which are common to both streams
shows = tvShows.join(movies)

# Finally counting the cast ids to find the number of actor and actress who worked in atleast one movie and one tv show in Nov 2019.
shows.count().pprint()

ssc.start()
ssc.awaitTermination()
