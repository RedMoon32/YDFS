from flask import Flask, jsonify
import threading
import time
import requests
import os

app = Flask(__name__)
DATA_NODE = "http://localhost:2020/"


@app.route("/ping")
def ping():
    return "Hello, Master Node is Alive!"


# parameters passed via url - /fileChunks?file=kek
@app.route("/fileLocations")
def file_locations():
    locations = [
        {'host': '10.0.0.11'},
        {'host': '10.0.0.12'},
        {'host': 'localhost'}
    ]
    return jsonify({'locations': locations})


def ping_data_nodes() :
    time.sleep(5)
    for i in range(10):
        print("Ping to datanode {datanode_host}".format(datanode_host=DATA_NODE), "has been sent")
        resp = requests.get(os.path.join(DATA_NODE, "ping"))
        if resp.status_code == 200:
            print(resp.content)
        else:
            print("Error")
        time.sleep(5)


if __name__ == "__main__":
    ping_thread = threading.Thread(target=ping_data_nodes)
    ping_thread.start()
    app.run(host='0.0.0.0', port=3030)
    ping_thread.join()
