from flask import Flask, request, Response
import os
from flask_api import status

app = Flask(__name__)

PORT = 2020
FILE_STORE = "./data"

# @TODO
# rewrite all methods using Flask Restful

"""
Data Node Api:
get /ping - get alive
get /fileSystem - return list of stored files
get /file?name return file content
put /file?name create new file if not exists and put there
post /filesystem remove all files from all 
delete /file?name remove file 
get /directory - get all files in directory
post /directory - create new directory


delete /fileSystem - remove all files
post /fileCopy?file,destination - init connection with other data node and send file there 
[by put /file?name] , only from master node

"""


@app.route("/ping")
def ping():
    return "Hello, Data Node is Alive!"


@app.route("/filesystem", methods=["GET"])
def filesystem():
    if request.method == "GET":
        # @TODO - recursively check file on filesystem
        def recursive_file_get(path):
            pass

        files = recursive_file_get(FILE_STORE)
        # for now - just files in current directory
        return os.listdir(FILE_STORE)


@app.route("/file", methods=["GET", "PUT", "DELETE"])
def file():
    filename = request.args["filename"]

    if './' in filename or '../' in filename:
        return Response('./ and ../ are not allowed in file path!', 400)

    fpath = os.path.join(FILE_STORE, filename)

    if not os.path.exists(os.path.dirname(fpath)):
        return Response(f"Folder {os.path.relpath(FILE_STORE, fpath)} not found", 404)

    if request.method == "GET":
        if not os.path.exists(fpath):
            return Response(f"File {filename} not found", 404)
        f = open(fpath, 'r')
        content = f.read()
        return Response(content, 200, mimetype='text/plain')

    elif request.method == "PUT":
        try:
            # надо ли?
            if os.path.exists(fpath):
                return Response(f"File {fpath} already exists", 400)
            f = open(fpath, 'wb')
            f.write(request.data)
            return Response(status=201)

        except Exception as e:
            print("Error:", str(e))
            return Response(f"Error opening file ", 400)

    elif request.method == "DELETE":
        if not os.path.exists(fpath):
            return Response(f"File {filename} not found", 404)
        else:
            os.remove(fpath)
            return Response(status=200)


def init_node():
    if not os.path.exists(FILE_STORE):
        os.mkdir(FILE_STORE)
    app.run(host='0.0.0.0', port=PORT)


if __name__ == "__main__":
    init_node()
