from flask import Flask

app = Flask(__name__)

"""
Data Node Api:
get /ping - get alive
get /fileSystem - return list of stored files
get /file?name return file content
put /file?name create new file if not exists and put there
post /filesystem remove all files from all 
delete /file?name remove file 

delete /fileSystem - remove all files
post /fileCopy?file,destination - init connection with other data node and send file there 
[by put /file?name] , only from master node

"""

@app.route("/ping")
def hello():
    return "Hello, Data Node is Alive!"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=2020)
