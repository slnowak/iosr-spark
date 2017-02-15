import json

import redis
from pyspark import SparkContext, SparkConf
from pyspark.streaming.kafka import KafkaUtils, OffsetRange


def entry_without_kafka_key(e):
    return e[1]


def kafka_rdd(spark_context, kafka_brokers='192.168.1.106:9092'):
    return KafkaUtils.createRDD(
        sc=spark_context,
        kafkaParams={'metadata.broker.list': kafka_brokers},
        offsetRanges=[OffsetRange(topic='flights', partition=0, fromOffset=0, untilOffset=49)]
    )


def populate_most_popular_airports(kafka_rdd, redis):
    def line_mapper(line):
        columns = line.split(',')
        return [(columns[17], 1), (columns[18], 1)]

    lines = kafka_rdd.map(entry_without_kafka_key)
    pairs = lines.flatMap(line_mapper)

    sums_by_airport = pairs.reduceByKey(lambda a, b: a + b)
    sorted = sums_by_airport.sortBy(lambda pair: pair[1], False)
    most_popular_airports = sorted.map(lambda pair: {pair[0]: pair[1]})

    redis.set('most_popular_airports', json.dumps(most_popular_airports.take(5)))


def populate_days_of_week_by_on_time_arrival(kafka_rdd, redis):
    def line_mapper(line):
        columns = line.split(',')
        return columns[4], float(columns[15] or 0)

    lines = kafka_rdd.map(entry_without_kafka_key)
    pairs = lines.map(line_mapper)

    avg_comp = pairs.aggregateByKey((0, 0), lambda a, b: (a[0] + b, a[1] + 1), lambda a, b: (a[0] + b[0], a[1] + b[1]))
    avg = avg_comp.mapValues(lambda v: v[0] / v[1])
    sorted = avg.sortBy(lambda pair: pair[1])
    days_of_week = sorted.map(lambda pair: {pair[0]: pair[1]})

    redis.set('days_of_week', days_of_week.collect())


def populate_carriers_by_departure(kafka_rdd, redis):
    def line_mapper(line):
        columns = line.split(',')
        if columns[17] == 'BWI':
            return [(columns[9], float(columns[25] or 0))]
        else:
            return []

    lines = kafka_rdd.map(entry_without_kafka_key)
    pairs = lines.flatMap(line_mapper)

    avg_comp = pairs.aggregateByKey((0, 0), lambda a, b: (a[0] + b, a[1] + 1), lambda a, b: (a[0] + b[0], a[1] + b[1]))
    avg = avg_comp.mapValues(lambda v: v[0] / v[1])
    sorted = avg.sortBy(lambda pair: pair[1])
    carriers_by_departure = sorted.map(lambda pair: {pair[0]: pair[1]})

    redis.set('carriers_by_departure', json.dumps(carriers_by_departure.take(5)))

def populate_mean_arrival_delays(kafka_rdd, redis):
    def line_mapper(line):
        columns = line.split(',')
        if columns[17] == 'JAN' and columns[18] == 'BWI':
            return [float(columns[15] or 0)]
        else:
            return []

    lines = kafka_rdd.map(entry_without_kafka_key)
    pairs = lines.flatMap(line_mapper)

    avg_comp = pairs.aggregate((0, 0), lambda a, b: (a[0] + b, a[1] + 1), lambda a, b: (a[0] + b[0], a[1] + b[1]))

    avg = None if avg_comp[1] == 0 else (avg_comp[0] / avg_comp[1])
    redis.set('mean_arrival_delays', json.dumps({'AVG (JAN -> BWI)': avg}))

# main
conf = SparkConf().setAppName('most_popular_airports')
sc = SparkContext(conf=conf)

db = redis.StrictRedis(host='redis', port=6379, db=0)

populate_most_popular_airports(kafka_rdd(sc), db)
populate_days_of_week_by_on_time_arrival(kafka_rdd(sc), db)
populate_carriers_by_departure(kafka_rdd(sc), db)
populate_mean_arrival_delays(kafka_rdd(sc), db)
