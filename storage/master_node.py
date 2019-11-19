from flask import Flask, jsonify, Response, request
from random import choice, choices
import threading
import time
import requests
import os
from storage.data_node_utils import DataNode
from storage.file_system import FileSystem, File

app = Flask(__name__)

data_nodes = []

fs = FileSystem()


@app.route("/ping")
def ping():
    return "Hello, Master Node is Alive!"


@app.route("/datanode", methods=["POST"])
def datanode():
    ip, port = request.args["ip"], request.args["port"]
    new_datanode = DataNode(ip, port)
    if new_datanode not in data_nodes:
        data_nodes.append(new_datanode)
        return Response(status=201)
    else:
        return Response(status=400)


@app.route("/file", methods=["POST", "GET"])
def file():
    filename = request.args["filename"]
    file: File = fs.get_file(filename)
    if request.method == "GET":
        if not file:
            return Response(status=404)
        else:
            return jsonify(file.serialize())
    elif request.method == "POST":
        file: File = fs.add_file(filename)
        if not file:
            return Response("File already exists", 400)
        return jsonify(choice(data_nodes).serialize())


@app.route("/directory", methods=["GET", "POST"])
def directory():
    dirname = request.args["name"]

    if dirname[-1] == "/":
        dirname = dirname[:-1]

    if dirname == "" or dirname[0] != "/":
        dirname = "/" + dirname

    if request.method == "POST":
        if dirname == "":
            return Response("Empty name not allowed", 400)
        if fs.dir_exists(dirname):
            return Response("Directory already exists", 400)
        if fs.get_file(dirname) is not None:
            return Response("File exists with given name", 400)
        if not fs.dir_exists(os.path.dirname(dirname)):
            return Response("Upper not exists", 400)
        fs.add_directory(dirname)
        return Response("Directory created", 201)

    elif request.method == "GET":
        if not fs.dir_exists(dirname):
            return Response("Directory does not exists", 400)
        return jsonify({'files': list(map(File.serialize, fs.get_files(dirname))),
                        'dirs': list(fs.get_subdirs(dirname))})


def ping_data_nodes():
    time.sleep(5)
    while True:

        for d in data_nodes:
            node_address = f"{d.ip}:{d.port}"
            print(f"Synchronisation with datanode {node_address}")
            resp = requests.get(os.path.join(node_address, "ping"))
            if resp.status_code == 200:
                print(f"Success - datanode {node_address} is alive")
            else:
                print(f"Datanode {node_address} synchronisation failed")
        time.sleep(5)


if __name__ == "__main__":
    ping_thread = threading.Thread(target=ping_data_nodes)
    ping_thread.start()
    app.run(host='0.0.0.0', port=3030)
    ping_thread.join()
