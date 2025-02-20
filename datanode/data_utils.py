import logging
import os
import sys
import threading
from logging.handlers import RotatingFileHandler

import requests
from flask import Flask

app = Flask(__name__)
DEBUG = True if os.getenv("DEBUG", "false") == "true" else False
PORT = int(os.getenv("PORT_D", 2020))
FILE_STORE = os.getenv("FILE_STORE", "./data")
MASTER_NODE = os.getenv("MASTER_NODE", "http://localhost:3030")


def create_log(app, node_name, debug=False):
    if not os.path.exists("./logs"):
        os.mkdir("./logs")
    open(f"./logs/{node_name}.log", "w+")
    file_handler = RotatingFileHandler(
        f"./logs/{node_name}.log", "a", 1 * 1024 * 1024, 10
    )
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]"
        )
    )
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)
    if debug:
        app.logger.setLevel(logging.DEBUG)
        file_handler.setLevel(logging.DEBUG)
    else:
        app.logger.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    app.logger.info(f"{node_name} startup")


def ping_master():
    import time

    while True:
        try:
            r = requests.post(os.path.join(MASTER_NODE, f"datanode?port={PORT}"))
        except:
            app.logger.error(f"Could not connect to Master Node - {MASTER_NODE}")
        time.sleep(5)


def init_node():
    if not os.path.exists(FILE_STORE):
        os.mkdir(FILE_STORE)
    try:
        requests.post(os.path.join(MASTER_NODE, f"datanode?port={PORT}"))
    except:
        app.logger.error(f"Could not connect to Master Node - {MASTER_NODE}")
        sys.exit(-1)
    ping_thread = threading.Thread(target=ping_master)
    ping_thread.start()
    app.run(host="0.0.0.0", port=PORT, debug=DEBUG)
    ping_thread.join()
