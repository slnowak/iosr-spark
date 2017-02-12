from flask import Flask
from flask import jsonify

from queries import AirportQueries

app = Flask(__name__)
queries = AirportQueries()


@app.route("/airports")
def most_popular_queries():
    return jsonify(queries.most_popular_airports())


if __name__ == "__main__":
    app.run()
