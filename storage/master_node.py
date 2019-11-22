from flask import Flask, jsonify, Response, request
from random import choices
import threading
import time
import requests
import os
from storage.data_node_utils import DataNode
from storage.file_system import FileSystem, File
from storage.utils import create_log
from math import ceil

app = Flask(__name__)

data_nodes = []

fs = FileSystem()
MAX_REQUEST_COUNT = 3
LOAD_FACTOR = 0.5  # fraction of datanodes to provide to a client at once


@app.errorhandler(Exception)
def handle_exception(e):
    status_code = 400
    if isinstance(e, FileNotFoundError):
        status_code = 404

    return Response(str(e), status=status_code)


def request_datanode(datanode, command, method):
    node_address = f"{datanode.ip}:{datanode.port}"
    resp = None
    for try_counter in range(MAX_REQUEST_COUNT):
        if method == "GET":
            resp = requests.get(os.path.join(node_address, command))
        elif method == "POST":
            resp = requests.post(os.path.join(node_address, command))
        elif method == "DELETE":
            resp = requests.delete(os.path.join(node_address, command))
        if resp.status_code == 200:
            return Response(status=200)
    # drop datanode if it does not respond
    drop_datanode(datanode)
    return resp


def drop_datanode(datanode):
    data_nodes.remove(datanode)
    app.logger.info(f"Removed not responding datanode {datanode.ip}:{datanode.port}")


def choose_datanodes():
    k = ceil(len(data_nodes) * LOAD_FACTOR)  # how much data_nodes to choose
    # Serialize each randomly chosen datanode and return a list of such datanodes
    return list(map(lambda node: node.serialize(), choices(data_nodes, k=k)))


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


@app.route("/filesystem", methods=["DELETE"])
def filesystem():
    if request.method == "DELETE":
        fs.__init__()
        for d in data_nodes:
            request_datanode(d, 'filesystem', request.method)
        if len(data_nodes) > 0:
            return Response("Storage is initialized and ready", 200)
        else:
            return Response("Storage is unavailable", 400)


@app.route("/file", methods=["POST", "GET", "PUT"])
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
        return jsonify({'datanodes': choose_datanodes(), 'file': file.serialize()})

    elif request.method == "PUT":
        destination = request.args["destination"]
        fs.move_file(filename, destination)
        return Response(status=200)


@app.route("/directory", methods=["GET", "POST"])
def directory():
    dirname = request.args["name"]

    if dirname[-1] == "/":
        dirname = dirname[:-1]

    if dirname == "" or dirname[0] != "/":
        dirname = "/" + dirname

    if request.method == "POST":
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
            app.logger.info(f"Synchronisation with datanode {node_address}")
            resp = requests.get(os.path.join(node_address, "ping"))
            if resp.status_code == 200:
                app.logger.info(f"Success - datanode {node_address} is alive")
            else:
                app.logger.info(f"Datanode {node_address} synchronisation failed")
        time.sleep(5)


if __name__ == "__main__":
    create_log(app, 'master_node')
    ping_thread = threading.Thread(target=ping_data_nodes)
    ping_thread.start()
    app.run(host='0.0.0.0', port=3030)
    ping_thread.join()
