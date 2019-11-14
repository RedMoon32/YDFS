from flask import Flask, jsonify, Response, request
from random import choice

from flask_api import status

import threading
import time
import requests
import os

"""
Master Node Api:
+ get /ping get alive
+ get /file?name return nodes where file is stored: [{ip:ip1,port:port1},{ip:ip2,port:port2}]
+ put /file?name return node location to put file (select random node)
delete /filesystem remove all files from all datanodes
delete /file?name remove file 
+ post /datanode? ip, port - add new data node to list
post /fileCopy?name,destination - copy file to destination
+ get /directory - get files from directory
...to be continued...


Background process:
replication check 
synchronise filesystem(file_mapper) - ask each data node for files it has
"""

app = Flask(__name__)

"""{file_name: [data_node1, data_node2, data_node3]}"""
file_mapper = {}
data_nodes = []


class DataNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = int(port)

    def __eq__(self, other):
        return self.ip == other.ip and self.port == other.port

    def serialize(self):
        return {"ip": self.ip,
                "port": self.port}


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
    if request.method == "GET":
        filename = request.args["filename"]
        if filename not in file_mapper:
            return Response(status=404)
        else:
            return jsonify(list(map(DataNode.serialize, file_mapper[filename])))
    elif request.method == "POST":
        return jsonify(choice(data_nodes).serialize())


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3030)
