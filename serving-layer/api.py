from flask import Flask
from flask import jsonify

from queries import AirportQueries

app = Flask(__name__)
queries = AirportQueries({'host': 'redis', 'port': 6379})


@app.route("/ex1")
def most_popular_airports():
    return jsonify(queries.most_popular_airports())


@app.route("/ex2")
def days_of_week_by_arrival_time():
    return jsonify(queries.days_of_week_by_on_time_arrival())


@app.route("/ex3")
def carriers_by_departure():
    return jsonify(queries.carriers_by_departure_performance())


@app.route("/ex4")
def mean_arrival_delays():
    return jsonify(queries.mean_arrival_delays())


if __name__ == "__main__":
    app.run(host='0.0.0.0')
