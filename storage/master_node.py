from flask import Flask, jsonify, Response, request
from random import choices
import threading
import time
import requests
import os

from urllib3 import HTTPConnectionPool
from requests.exceptions import ConnectionError
from storage.data_node_utils import DataNode
from storage.file_system import FileSystem, File
from storage.utils import create_log
from math import ceil

app = Flask(__name__)

data_nodes = []

fs = FileSystem()
MAX_REQUEST_COUNT = 3
LOAD_FACTOR = 0.5  # fraction of datanodes to provide to a client at once


def request_datanode(datanode, command, method):
    node_address = f"{datanode.ip}:{datanode.port}"
    resp = None
    for try_counter in range(MAX_REQUEST_COUNT):
        try:
            if method == "GET":
                resp = requests.get(os.path.join(node_address, command))
            elif method == "POST":
                resp = requests.post(os.path.join(node_address, command))
            elif method == "DELETE":
                resp = requests.delete(os.path.join(node_address, command))
            if resp.status_code == 200:
                return Response(status=200)
        except HTTPConnectionPool:
            pass
    # drop datanode if it does not respond
    drop_datanode(datanode)
    return resp


def drop_datanode(datanode):
    data_nodes.remove(datanode)
    app.logger.info(f"Removed not responding datanode {datanode.ip}:{datanode.port}")


def choose_datanodes():
    k = ceil(len(data_nodes) * LOAD_FACTOR) # how much data_nodes to choose
    # Serialize each randomly chosen datanode and return a list of such datanodes
    return list(map(lambda node: node.serialize(), choices(data_nodes, k=k)))


@app.route("/ping")
def ping():
    return "Hello, Master Node is Alive!"


@app.route("/datanode", methods=["POST"])
def datanode():
    host, port = request.args["host"], request.args["port"]
    new_datanode = DataNode(host, port)
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
        try:
            file: File = fs.add_file(filename)
        except Exception as e:
            print(str(e))
            return Response(str(e), 400)
        return jsonify({'datanodes': choose_datanodes(), 'file': file.serialize()})

    elif request.method == "PUT":
        if request.args["op"] == "mv":
            if not file:
                return Response("No such file", 404)
            destination = request.args["destination"]
            new_file_name = os.path.join(destination, os.path.basename(filename))
            if fs.dir_exists(destination) and not fs.file_exists(new_file_name):
                fs.rename_file(filename, new_file_name)
                return Response(status=200)
            else:
                return Response("Output folder path does not exists", 404)
        elif request.args["op"] == "cp":
            source = filename
            target = request.args["target"]
            source_file: File = file
            if not source_file:
                return Response("No such source file", 404)
            target_file: File = fs.get_file(target)
            if not target_file:
                return Response("Target file with this name already exists", 400)
            if not fs.dir_exists(os.path.dirname(target)):
                return Response("Target directory doesn't exists", 404)
            target_file = fs.copy_file(source_file, target)
            for node in target_file.nodes:
                node_address = f"{node.ip}:{node.port}"
                app.logger.info(f"Synchronisation with datanode {node_address}")
                resp = requests.put(os.path.join(node_address, f"file?filename={source_file.id}&target={target_file.id}"))
                if resp.status_code == 201:
                    app.logger.info(f"Success - datanode {node_address} copied file")
                else:
                    app.logger.info(f"Datanode {node_address} failed to copy file")
                    target_file.nodes.remove(node_address)
            if not target_file.nodes:
                return Response("All datanodes failed to copy file", 400)
            return Response(status=201)
        else:
            return Response("Wrong operation code, request should include 'op=<operation_code>'", 400)



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
        return Response(f"Directory '{dirname}' created", 201)

    elif request.method == "GET":
        if not fs.dir_exists(dirname):
            return Response(f"Directory '{dirname}' does not exist", 400)
        return jsonify({'files': list(map(File.serialize, fs.get_files(dirname))),
                        'dirs': list(fs.get_subdirs(dirname))})


@app.route("/file_created", methods=["POST"])
def event():
    file_id, ip, port = int(request.args["file_id"]), f"http://{request.remote_addr}", request.args["port"]
    dnode = DataNode(ip, port)
    file: File = fs.get_file_by_id(file_id)
    if dnode not in data_nodes or file is None:
        return Response(status=404)
    else:
        file.nodes.append(dnode)
        return Response(status=200)


def ping_data_nodes():
    time.sleep(5)
    while True:
        for d in data_nodes:
            node_address = f"{d.ip}:{d.port}"
            app.logger.info(f"Synchronisation with datanode {node_address}")
            try:
                resp = requests.get(os.path.join(node_address, "ping"))
                app.logger.info(f"Success - datanode {node_address} is alive")
            except ConnectionError as e:
                app.logger.info(f"Datanode {node_address} synchronisation failed")
                drop_datanode(d)
        time.sleep(5)


if __name__ == "__main__":
    create_log(app, 'master_node')
    ping_thread = threading.Thread(target=ping_data_nodes)
    ping_thread.start()
    app.run(host='0.0.0.0', port=3030)
    ping_thread.join()
