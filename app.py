from flask import Flask

app = Flask(__name__)


# some fake data
@app.route("/data")
def data():
    return {
        "data": [
            "data1",
            "data2",
            "etc",
        ]
    }


if __name__ == "__main__":
    app.run(debug=True)
