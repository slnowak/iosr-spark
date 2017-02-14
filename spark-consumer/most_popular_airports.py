import redis
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming.kafka import KafkaUtils, OffsetRange

# utils
def line_mapper(line):
    columns = line.split(',')
    return [(columns[17], 1), (columns[18], 1)]

def entry_without_kafka_key(e):
    return e[1]

# db access
db = redis.StrictRedis(host='redis', port=6379, db=0)

# main
conf = SparkConf().setAppName('most_popular_airports')
sc = SparkContext(conf=conf)

rdd = KafkaUtils.createRDD(
    sc=sc,
    kafkaParams={'metadata.broker.list': '192.168.1.106:9092'},
    offsetRanges=[OffsetRange(topic='flights', partition=0, fromOffset=0, untilOffset=49)]
)

lines = rdd.map(entry_without_kafka_key)
pairs = lines.flatMap(line_mapper)

sums_by_airport = pairs.reduceByKey(lambda a, b: a + b)
sorted = sums_by_airport.sortBy(lambda pair: pair[1], False)
most_popular_airports = sorted.map(lambda pair: {pair[0] : pair[1]})

db.set('most_popular_airports', json.dumps(most_popular_airports.take(5)))
