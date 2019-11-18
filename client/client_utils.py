import requests
from flask import Response
from os.path import isabs, join, normpath

MAX_REQUEST_COUNT = 3
pwd = '/'


def check_response(resp):
    if resp.status_code == 200:
        print(resp.content.decode())
        return 0
    else:
        print("Error:", resp.content.decode())
        return 1


def check_args(command, args, missing_operands):
    if len(args) < 2:
        print(f'{command}: missing {missing_operands[0]} operand')
        return 1
    for i in range(1, len(missing_operands)):
        if len(args) < i + 2:
            print(f"{command}: missing {missing_operands[i]} operand after '{args[i]}'")
            return 1
    # Check if extra operands are present
    if len(args) - 1 > len(missing_operands):
        print(f'{command}: extra operands are present, expected [{len(missing_operands)}] - got [{len(args) - 1}]')
        return 1
    return 0


def request_datanodes(datanodes, command, method, data=None):
    resp = None
    for datanode in datanodes:
        node_address = f"{datanode['ip']}:{datanode['port']}"
        for try_counter in range(MAX_REQUEST_COUNT):
            if method == "GET":
                resp = requests.get(join(node_address, command), data=data)
            elif method == "POST":
                resp = requests.post(join(node_address, command), data=data)
            elif method == "DELETE":
                resp = requests.delete(join(node_address, command), data=data)
            if resp.status_code == 200:
                return Response(status=200)
    return resp


def read_file(path):
    try:
        return open(path, 'rb').read()
    except OSError as e:
        print(e)
        return None


def join_path(filename, destination):
    if isabs(destination):
        return normpath(join(destination, filename))
    else:
        return normpath(join(pwd, destination, filename))


def make_abs(path):
    if isabs(path):
        return normpath(path)
    else:
        return normpath(join(pwd, path))
