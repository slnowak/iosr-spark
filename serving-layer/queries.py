import redis

class AirportQueries:
    def __init__(self):
        self.db = redis.StrictRedis(host='localhost', port=6379, db=0)

    def most_popular_airports(self):
        return self.db.get('most_popular_airports')
