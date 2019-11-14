from flask import Flask
import os

app = Flask(__name__)

PORT = 2020
FILE_STORE = "./data"

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


def init_node():
    if not os.path.exists(FILE_STORE):
        os.mkdir(FILE_STORE)

    app.run(host='0.0.0.0', port=PORT)


if __name__ == "__main__":
    init_node()
