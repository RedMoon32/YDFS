import logging
import os
from logging.handlers import RotatingFileHandler
from math import ceil
from random import choices

import requests
from flask import Flask, Response

from file_system import FileSystem

app = Flask(__name__)
fs = FileSystem()
data_nodes = []
MAX_REQUEST_COUNT = 3
LOAD_FACTOR = 0.5  # fraction of datanodes to provide to a client at once
REPLICATION_FACTOR = 3


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
            elif method == "PUT":
                resp = requests.put(os.path.join(node_address, command))
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
    app.logger.info(f"Removed not responding Data Node {datanode.ip}:{datanode.port}")


def choose_datanodes():
    k = ceil(len(data_nodes) * LOAD_FACTOR)  # how much data_nodes to choose
    # Serialize each randomly chosen datanode and return a list of such datanodes
    return list(map(lambda node: node.serialize(), choices(data_nodes, k=k)))


def choose_datanodes_for_replication(nodes_with_file):
    return choices([x for x in data_nodes if x not in nodes_with_file], k=REPLICATION_FACTOR - len(nodes_with_file))


def create_log(app, node_name, debug=False):
    if not os.path.exists("logs"):
        os.mkdir("logs")
    open(f"logs/{node_name}.log", "w+")
    file_handler = RotatingFileHandler(
        f"logs/{node_name}.log", "a", 1 * 1024 * 1024, 10
    )
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]"
        )
    )
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    if debug:
        app.logger.setLevel(logging.DEBUG)
        file_handler.setLevel(logging.DEBUG)
    else:
        app.logger.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    app.logger.info(f"{node_name} startup")
