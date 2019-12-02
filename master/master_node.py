import os
import threading
import time

import requests
from flask import jsonify, Response, request
from requests.exceptions import ConnectionError

from file_system import File, DataNode
from master_utils import app, create_log, data_nodes, request_datanode, choose_datanodes, \
    choose_datanodes_for_replication, drop_datanode, fs, REPLICATION_FACTOR

DEBUG = False
PORT = 3030
MAX_DATANODE_CAPACITY = 5 * 1024 * 1024 * 1024  # max available memory on each Data Node
free_memory = 0  # free storage memory in bytes


@app.route("/ping")
def ping():
    return "Hello, Master Node is Alive!"


@app.route("/status")
def status():
    app.logger.info(f"Sent filesystem data to a client")
    return jsonify({
        "Free Space": f"{free_memory / 1024 / 1024:.3f} MB",
        "Master Node Address": f"{request.remote_addr}:{PORT}",
        "Available Data Nodes": [node.serialize() for node in data_nodes]
    })


@app.route("/datanode", methods=["POST"])
def datanode():
    host, port = f"http://{request.remote_addr}", request.args["port"]
    new_datanode = DataNode(host, port)
    if new_datanode not in data_nodes:
        data_nodes.append(new_datanode)
        app.logger.info(f"Add a new Data Node {new_datanode.ip}:{new_datanode.port}")
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
            message = f"Storage is initialized and ready, available disk space is " \
                      f"{MAX_DATANODE_CAPACITY * len(data_nodes) / 1024 / 1024:.3f} MB."
            app.logger.info(message)
            return Response(message, 200)
        else:
            app.logger.info("Storage is unavailable")
            return Response("Storage is unavailable", 400)


@app.route("/file", methods=["POST", "GET", "PUT", "DELETE"])
def file():
    filename = request.args["filename"]
    file: File = fs.get_file(filename)

    if request.method == "GET":
        if not file:
            app.logger.info(f"File '{filename}' data was requested but file not found")
            return Response(f"file '{filename}' not found", status=404)
        else:
            app.logger.info(f"Sent the file '{filename}' data to a client")
            file.file_info["last_accessed"] = time.ctime()
            return jsonify({"file": file.serialize()})

    elif request.method == "POST":
        file: File = fs.add_file(filename)
        app.logger.info(f"Added file '{filename}' to the filesystem")
        return jsonify({"datanodes": choose_datanodes(), "file": file.serialize()})

    elif request.method == "PUT":
        destination = request.args["destination"]
        fs.move_file(filename, destination)
        app.logger.info(f"File '{filename}' was moved to '{destination}'")
        return Response(f"file '{filename}' was moved to '{destination}'", 200)

    elif request.method == "DELETE":
        if not file:
            app.logger.info(f"File '{filename}' was requested to delete but file not found")
            return Response(f"file '{filename}' not found", status=404)
        else:
            for dnode in file.nodes:
                request_datanode(dnode, f"file?filename={file.id}", "DELETE")
            fs.remove_file(filename)
            app.logger.info(f"File '{filename}' was deleted")
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
        app.logger.info(f"Directory '{dirname}' created")
        return Response(f"directory '{dirname}' created", 201)

    elif request.method == "GET":
        if not fs.dir_exists(dirname):
            app.logger.info(f"Directory '{dirname}' data was requested but it does not exist")
            return Response(f"directory '{dirname}' does not exist", 404)
        app.logger.info(f"Sent the directory '{dirname}' data to a client")
        return jsonify(
            {
                "files": list(map(File.serialize, fs.get_files(dirname))),
                "dirs": list(fs.get_subdirs(dirname)),
            }
        )
    elif request.method == "DELETE":
        if not fs.dir_exists(dirname):
            app.logger.info(f"Directory '{dirname}' was requested to delete but it does not exist")
            return Response(f"directory '{dirname}' does not exist", 404)
        if dirname == '/':
            app.logger.info(f"Directory '{dirname}' was requested to delete but root directory cannot be deleted")
            return Response("cannot remove root directory", 400)
        rm_list = fs.remove_dir(dirname)
        for file in rm_list:
            for dnode in file.nodes:
                request_datanode(dnode, f"file?filename={file.id}", "DELETE")
            fs.remove_file(file.name)
        app.logger.info(f"Directory '{dirname}' and all its sub-folders and files were deleted")
        return Response(
            f"directory '{dirname}' and all its sub-folders and files were deleted",
            status=200,
        )


def ping_data_nodes():
    time.sleep(5)
    while True:

        total_occupied = 0
        for cur_node in data_nodes:
            files = fs.get_all_files()
            file_ids = {"files": fs.get_all_ids()}

            node_address = f"{cur_node.ip}:{cur_node.port}"
            app.logger.debug(f"Synchronisation with datanode {node_address}")
            try:
                resp = requests.get(
                    os.path.join(node_address, "filesystem"), json=file_ids
                )
                app.logger.debug(f"Success - datanode {node_address} is alive")
                data_file_ids = resp.json()["files"]

                file_sizes = resp.json()["file_sizes"]
                total_occupied += sum(file_sizes)

                for (file_id, file_size) in zip(data_file_ids, file_sizes):
                    file = fs.get_file_by_id(int(file_id))
                    # update info that data node has some new file
                    if file is None:
                        app.logger.info(
                            f"Sent request to delete unknown file {file_id} from the Data Node {node_address}"
                        )
                        request_datanode(cur_node, f"file?filename={file_id}", "DELETE")
                        continue
                    if not cur_node in file.nodes:
                        app.logger.info(
                            f"New file found on the Data Node {node_address} - {file.name}"
                        )
                        file.nodes.append(cur_node)
                    file.file_info["size"] = file_size
                for file in files:
                    # update info that data node does not have some file
                    if cur_node in file.nodes and file.id not in data_file_ids:
                        app.logger.info(
                            f"File {file.name} not found on the Data Node {node_address}, deleting from database"
                        )
                        file.nodes.remove(cur_node)

            except ConnectionError as e:
                app.logger.info(f"Data Node '{node_address}' synchronisation failed")
                drop_datanode(cur_node)

        global free_memory
        free_memory = len(data_nodes) * MAX_DATANODE_CAPACITY - total_occupied
        time.sleep(5)


def replication_check():
    time.sleep(6)
    while True:
        for file in fs.get_all_files:
            if len(file.nodes) < REPLICATION_FACTOR:
                nodes = choose_datanodes_for_replication(file.nodes)
                for i in range(len(nodes)):
                    target_node, source_node = nodes[i], file.nodes[i % len(file.nodes)]
                    source_address = f"{source_node.ip}:{source_node.port}"
                    request_datanode(target_node, f"?sourcenode={source_address}&filename={file.id}", "PUT")
        time.sleep(5)


if __name__ == "__main__":
    create_log(app, "master_node", debug=DEBUG)
    ping_thread = threading.Thread(target=ping_data_nodes)
    ping_thread.start()
    repl_thread = threading.Thread(target=replication_check)
    repl_thread.start()
    app.run(host="0.0.0.0", port=PORT, debug=DEBUG)
    ping_thread.join()
    repl_thread.join()
