import ast

import redis

class AirportQueries:
    def __init__(self, redis_cfg):
        self.db = redis.StrictRedis(host=redis_cfg['host'], port=redis_cfg['port'], db=0)

    def most_popular_airports(self):
        return self.__json_or_none(self.db.get('most_popular_airports'))

    def days_of_week_by_on_time_arrival(self):
        return self.__json_or_none(self.db.get('days_of_week'))

    def carriers_by_departure_performance(self):
        return self.__json_or_none(self.db.get('carriers_by_departure'))

    def mean_arrival_delays(self):
        return self.__json_or_none(None)

    @staticmethod
    def __json_or_none(value):
        return None if value is None else ast.literal_eval(value)
