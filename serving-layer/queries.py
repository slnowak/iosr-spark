import redis


class AirportQueries:
    def __init__(self, redis_cfg):
        self.db = redis.StrictRedis(host=redis_cfg['host'], port=redis_cfg['port'], db=0)

    def most_popular_airports(self):
        return self.db.get('most_popular_airports')
