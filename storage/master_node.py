from flask import Flask, jsonify, Response, request
from random import choices
import threading
import time
import requests
import os

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
        try:
            if method == "GET":
                resp = requests.get(os.path.join(node_address, command))
            elif method == "POST":
                resp = requests.post(os.path.join(node_address, command))
            elif method == "DELETE":
                resp = requests.delete(os.path.join(node_address, command))
            return resp
        except Exception as e:
            app.logger.info(f"DataNode {node_address} connection failed")
    drop_datanode(datanode)
    return None


def drop_datanode(datanode):
    for file in fs.get_all_files():
        if datanode in file.nodes:
            file.nodes.remove(datanode)
            app.logger.info(
                f"Removing file {file.name} in database from {datanode.ip}:{datanode.port}"
            )
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
    host, port = f"http://{request.remote_addr}", request.args["port"]
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
            request_datanode(d, "filesystem", request.method)
        if len(data_nodes) > 0:
            return Response("Storage is initialized and ready", 200)
        else:
            return Response("Storage is unavailable", 400)


@app.route("/file", methods=["POST", "GET", "PUT", "DELETE"])
def file():
    filename = request.args["filename"]
    file: File = fs.get_file(filename)

    if request.method == "GET":
        if not file:
            return Response(f"file {filename} not found", status=404)
        else:
            return jsonify({"file": file.serialize()})

    elif request.method == "POST":
        file: File = fs.add_file(filename)
        return jsonify({"datanodes": choose_datanodes(), "file": file.serialize()})

    elif request.method == "PUT":
        op = request.args["operation"]
        if op == "mv":
            destination = request.args["destination"]
            fs.move_file(filename, destination)
            return Response(f"file '{filename}' was moved to '{destination}'", 200)
        elif op == "cp":
            path = request.args["path"]
            fs.copy_file(filename, path)
            if not file:
                return Response(f"file {filename} not found", status=404)
            target_file = fs.get_file(path)
            if len(target_file.nodes) == 0:
                return Response(f"file {filename} location is not available for copying, wait a bit", status=404)

            for node in target_file.nodes:
                node_address = f"{node.ip}:{node.port}"
                app.logger.info(f"Synchronisation with datanode {node_address}")
                resp = requests.put(os.path.join(node_address, f"file?filename={file.id}&"
                                                               f"target={target_file.id}&"
                                                               f"source_node={node_address}"))
                if resp.status_code == 201:
                    app.logger.info(f"Success - datanode {node_address} copied file")
                else:
                    app.logger.info(f"Datanode {node_address} failed to copy file")
                    target_file.nodes.remove(node_address)
            if not target_file.nodes:
                return Response("All datanodes failed to copy file", 400)

            return Response(f"file '{filename}' was copied to '{path}'", 201)
        else:
            return Response("Wrong operation code, request should include 'op=<operation_code>'", 400)

    elif request.method == "DELETE":
        if not file:
            return Response(f"file '{filename}' not found", status=404)
        else:
            for dnode in file.nodes:
                request_datanode(dnode, f"file?filename={file.id}", "DELETE")
            fs.remove_file(filename)
            return Response(f"file '{filename}' was deleted", 200)


@app.route("/directory", methods=["GET", "POST", "DELETE"])
def directory():
    dirname = request.args["name"]

    if dirname[-1] == "/":
        dirname = dirname[:-1]

    if dirname == "" or dirname[0] != "/":
        dirname = "/" + dirname

    if request.method == "POST":
        fs.add_directory(dirname)
        return Response(f"directory '{dirname}' created", 201)

    elif request.method == "GET":
        if not fs.dir_exists(dirname):
            return Response(f"directory '{dirname}' does not exist", 404)
        return jsonify(
            {
                "files": list(map(File.serialize, fs.get_files(dirname))),
                "dirs": list(fs.get_subdirs(dirname)),
            }
        )
    elif request.method == "DELETE":
        if not fs.dir_exists(dirname):
            return Response(f"directory '{dirname}' does not exist", 404)
        if dirname == '/':
            return Response("cannot remove root directory", 400)
        rm_list = fs.remove_dir(dirname)
        for file in rm_list:
            for dnode in file.nodes:
                request_datanode(dnode, f"file?filename={file.id}", "DELETE")
            fs.remove_file(file.name)
        return Response(
            f"directory '{dirname}' and all its sub-folders and files were deleted",
            status=200,
        )


def ping_data_nodes():
    time.sleep(5)
    while True:

        for cur_node in data_nodes:
            files = fs.get_all_files()
            file_ids = {"files": fs.get_all_ids()}

            node_address = f"{cur_node.ip}:{cur_node.port}"
            app.logger.info(f"Synchronisation with datanode {node_address}")
            try:
                resp = requests.get(
                    os.path.join(node_address, "filesystem"), json=file_ids
                )
                app.logger.info(f"Success - datanode {node_address} is alive")
                data_file_ids = resp.json()["files"]
                for file_id in data_file_ids:
                    file = fs.get_file_by_id(int(file_id))
                    # update info that data node has some new file
                    if file is None:
                        app.logger.info(
                            f"Sent request to delete unknown file {file_id} on datanode {node_address}"
                        )
                        request_datanode(cur_node, f"file?filename={file_id}", "DELETE")
                        continue
                    if not cur_node in file.nodes:
                        app.logger.info(
                            f"New file found on datanode {node_address} - {file.name}"
                        )
                        file.nodes.append(cur_node)
                for file in files:
                    # update info that data node does not have some file
                    if cur_node in file.nodes and file.id not in data_file_ids:
                        app.logger.info(
                            f"File {file.name} not found on datanode, deleting from database"
                        )
                        file.nodes.remove(cur_node)

            except ConnectionError as e:
                app.logger.info(f"datanode '{node_address}' synchronisation failed")
                drop_datanode(cur_node)
        time.sleep(5)


if __name__ == "__main__":
    create_log(app, "master_node")
    ping_thread = threading.Thread(target=ping_data_nodes)
    ping_thread.start()
    app.run(host="0.0.0.0", port=3030)
    ping_thread.join()
