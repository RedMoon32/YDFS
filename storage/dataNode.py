from flask import Flask

app = Flask(__name__)


@app.route("/ping")
def hello():
    return "Hello, Data Node is Alive!"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=2020)
