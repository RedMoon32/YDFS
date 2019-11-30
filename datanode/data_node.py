import sys

import requests
from flask import Flask, request, Response, jsonify
import os
import shutil


app = Flask(__name__)

PORT = 2020
FILE_STORE = "./data"
MASTER_NODE = os.getenv("MASTER_NODE", "http://localhost:3030")


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


@app.route("/file", methods=["GET", "POST", "DELETE"])
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

    elif request.method == "DELETE":
        if not os.path.exists(fpath):
            return Response(f"file not '{fpath}' found", 404)
        else:
            os.remove(fpath)
            return Response(status=200)


def init_node():
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
