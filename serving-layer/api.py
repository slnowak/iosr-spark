from flask import Flask
from flask import jsonify

from queries import AirportQueries

app = Flask(__name__)
queries = AirportQueries({'host': 'redis', 'port': 6379})


@app.route("/airports")
def most_popular_queries():
    return jsonify(queries.most_popular_airports())


if __name__ == "__main__":
    app.run(host='0.0.0.0')
