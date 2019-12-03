import os
import shutil
import threading
import time

import requests
from flask import request, Response, jsonify

from data_utils import create_log, app, FILE_STORE, DEBUG, init_node



@app.route("/ping")
def ping():
    return Response("Data Node is Alive")


@app.errorhandler(Exception)
def handle_exception(e):
    status_code = 400
    if isinstance(e, FileNotFoundError):
        status_code = 404
    app.logger.error(
        "Error during working:" + str(e) + ", request by url:" + str(request.url)
    )
    return Response(str(e), status=status_code)


@app.route("/filesystem", methods=["DELETE", "GET"])
def filesystem():
    if request.method == "DELETE":
        try:
            # remove the storage dir with all its contents and create it anew
            shutil.rmtree(FILE_STORE, ignore_errors=True)
            os.mkdir(FILE_STORE)
            app.logger.info("Storage is cleared")
            return Response(status=200)
        except Exception as e:
            app.logger.info("Error clearing storage")
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
        app.logger.debug("Sent storage data to a Master Node")
        return jsonify(
            {
                "files": [int(fid) for fid in os.listdir(FILE_STORE)],
                "file_sizes": [
                    os.path.getsize(os.path.join(FILE_STORE, fid))
                    for fid in os.listdir(FILE_STORE)
                ],
            }
        )


@app.route("/file", methods=["GET", "POST", "PUT", "DELETE"])
def file():
    filename = request.args["filename"]

    if "/" in filename:
        return Response("/ are not allowed in file name!", 400)

    fpath = os.path.join(FILE_STORE, filename)

    if request.method == "GET":
        if not os.path.exists(fpath):
            app.logger.info(f"File '{fpath}' content was requested but file not found")
            return Response(f"file '{fpath}' not found", 404)
        f = open(fpath, "r")
        content = f.read()
        app.logger.info(f"Sent the file '{fpath}' content to a client")
        return Response(content, 200, mimetype="text/plain")

    elif request.method == "POST":
        try:
            if os.path.exists(fpath):
                app.logger.info(
                    f"File '{fpath}' was requested to save but file already exists"
                )
                return Response(f"file '{fpath}' already exists", 400)
            f = open(fpath, "wb")
            f.write(request.data)
            app.logger.info(f"Saved new file '{fpath}' to the storage")
            return Response(status=201)

        except Exception as e:
            app.logger.info(f"File '{fpath}' found but could not read")
            return Response(f"error opening file '{fpath}'", 400)

    # this is for coping file from this or other node, under this, or other name 🤡
    # format is:
    # /file?sourcenode=<source_node_address> – optional, if none, means that copy from this node
    #      &filename=<source_file> – name of the file which should be copied
    elif request.method == "PUT":
        try:
            target = filename
            if "/" in target:
                return Response("/ are not allowed in file name!", 400)
            target_path = os.path.join(FILE_STORE, target)
            if os.path.exists(target_path):
                app.logger.info(
                    f"File '{fpath}' was requested to copy but target file '{target_path}' already exists"
                )
                return Response(f"File already exists", 400)

            source_node = request.args["source_node"]
            resp = requests.get(os.path.join(source_node, f"file?filename={filename}"))
            if resp.status_code == 200:
                try:
                    f = open(target_path, "wb")
                    f.write(resp.content)
                except:
                    app.logger.info(f"Could not save replication of a file {fpath}")
                    return Response("Error while saving on local filesystem", 400)
            else:
                app.logger.info(
                    "Error with requesting file from source_node, it returned: "
                    + str(resp.status_code)
                )
                return Response(
                    "Error with requesting file from source_node, it returned: ",
                    resp.status_code,
                    400,
                )

            app.logger.info(
                f"File '{fpath}' was replicated from source node '{source_node}'"
            )
            return Response(status=201)
        except Exception as e:
            return Response(f"Error during something ", 400)

    elif request.method == "DELETE":
        if not os.path.exists(fpath):
            app.logger.info(
                f"File '{fpath}' was requested for deletion but file not found"
            )
            return Response(f"file not '{fpath}' found", 404)
        else:
            os.remove(fpath)
            app.logger.info(f"File '{fpath}' was deleted from storage")
            return Response(status=200)


if __name__ == "__main__":
    create_log(app, "data_node", debug=DEBUG)
    init_node()
