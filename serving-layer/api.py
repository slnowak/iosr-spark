from flask import Flask
from flask import request
from flask import jsonify

from queries import AirportQueries

app = Flask(__name__)
queries = AirportQueries()


@app.route("/airports")
def most_popular_queries():
    limit = int(request.args.get('limit')) if request.args.get('limit') else 5
    return jsonify(queries.most_popular_airports(limit))


if __name__ == "__main__":
    app.run()
