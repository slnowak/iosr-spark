import redis
import json

class AirportQueries:
    def __init__(self, redis_cfg):
        self.db = redis.StrictRedis(host=redis_cfg['host'], port=redis_cfg['port'], db=0)

    def most_popular_airports(self):
        return self.__json_or_none(self.db.get('most_popular_airports'))

    def __json_or_none(self, value):
        return None if value is None else json.loads(value)
