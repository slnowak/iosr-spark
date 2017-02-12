# sh $SPARK_HOME/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 top_5_airports.py

from pyspark import SparkContext, SparkConf
from pyspark.streaming.kafka import KafkaUtils, OffsetRange


def line_mapper(line):
    columns = line.split(',')
    return [(columns[17], 1), (columns[18], 1)]


def entry_without_kafka_key(e):
    return e[1]


conf = SparkConf().setAppName('IOSR-1')
sc = SparkContext(conf=conf)

rdd = KafkaUtils.createRDD(
    sc=sc,
    kafkaParams={'metadata.broker.list': 'localhost:9092'},
    offsetRanges=[OffsetRange(topic='flights', partition=0, fromOffset=0, untilOffset=1936758)]
)

lines = rdd.map(entry_without_kafka_key)
pairs = lines.flatMap(line_mapper)

sums_by_airport = pairs.reduceByKey(lambda a, b: a + b)
sorted = sums_by_airport.sortBy(lambda pair: pair[1], False)

print sorted.take(5)
