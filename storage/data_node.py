import sys

import requests
from flask import Flask, request, Response, jsonify
import os
import shutil
from storage.utils import create_log

app = Flask(__name__)

PORT = 2020
FILE_STORE = "./data"
MASTER_NODE = "http://localhost:3030/"


@app.route("/ping")
def ping():
    return Response("Master Node is Alive")


@app.route("/filesystem", methods=["DELETE", "GET"])
def filesystem():
    if request.method == "DELETE":
        try:
            # remove the storage dir with all its contents and create it anew
            shutil.rmtree(FILE_STORE, ignore_errors=True)
            os.mkdir(FILE_STORE)
            return Response(status=200)
        except Exception as e:
            return Response(f"Error clearing storage", 400)

    elif request.method == "GET":
        if "files" not in request.json:
            return Response(status=400)
        file_ids = request.json["files"]
        for fid in os.listdir(FILE_STORE):
            if int(fid) not in file_ids:
                os.remove(os.path.join(FILE_STORE, fid))
                app.logger.info(
                    f"Deleting File with fid={fid} as file not found on master"
                )
        return jsonify({"files": [int(fid) for fid in os.listdir(FILE_STORE)]})


@app.route("/file", methods=["GET", "POST", "PUT", "DELETE"])
def file():
    filename = request.args["filename"]

    if "/" in filename:
        return Response("/ are not allowed in file name!", 400)

    fpath = os.path.join(FILE_STORE, filename)

    if request.method == "GET":
        if not os.path.exists(fpath):
            return Response(f"file '{fpath}' not found", 404)
        f = open(fpath, "r")
        content = f.read()

        return Response(content, 200, mimetype="text/plain")

    elif request.method == "POST":
        try:
            if os.path.exists(fpath):
                return Response(f"file '{fpath}' already exists", 400)
            f = open(fpath, "wb")
            f.write(request.data)

            return Response(status=201)

        except Exception as e:
            return Response(f"error opening file '{fpath}'", 400)

    # this is for coping file from this or other node, under this, or other name 🤡
    # format is:
    # /file?sourcenode=<source_node_address> – optional, if none, means that copy from this node
    #      &filename=<source_file> – name of the file which should be copied
    #      &target=<target_file> – optional, if none, same name, as original
    elif request.method == "PUT":
        try:
            target = request.args["target"]
            # for replication ease
            if target is None:
                target = filename
            if '/' in target:
                return Response('/ are not allowed in file name!', 400)
            target_path = os.path.join(FILE_STORE, target)
            if os.path.exists(target_path):
                return Response(f"File already exists", 400)

            source_node = request.args["source_node"]
            resp = requests.get(os.path.join(source_node, f"file?filename={filename}"))
            if resp.status_code == 200:
                try:
                    f = open(target_path, "wb")
                    f.write(resp.content)
                except:
                    return Response("Error while saving on local filesystem", 400)
            else:
                return Response("Error with requesting file from source_node, it returned: " + resp.status_code,
                                400)

            return Response(status=201)
        except Exception as e:
            return Response(f"Error during something ", 400)

    elif request.method == "DELETE":
        if not os.path.exists(fpath):
            return Response(f"file not '{fpath}' found", 404)
        else:
            os.remove(fpath)
            return Response(status=200)


def init_node():
    create_log(app, "data_node")
    if not os.path.exists(FILE_STORE):
        os.mkdir(FILE_STORE)
    try:
        requests.post(os.path.join(MASTER_NODE, "datanode?port=2020"))
    except:
        app.logger.error(f"Could not connect to Master Node - {MASTER_NODE}")
        sys.exit(-1)
    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    init_node()
