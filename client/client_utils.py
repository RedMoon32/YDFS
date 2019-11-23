import requests
from flask import Response
from os.path import isabs, join, normpath

MAX_REQUEST_COUNT = 3
pwd = '/'


def check_response(resp):
    if resp.status_code == 200:
        print(resp.content.decode())
        return True
    else:
        print("Error:", resp.content.decode())
        return False


def check_args(command: str, args: list, required_operands: list):
    """
    Check that number of arguments is correct.
    :param command: CLI command to check. Used for user prompt.
    :param args: received arguments
    :param missing_operands:
    :return:
    """
    if len(args) < 2:
        print(f'{command}: missing {required_operands[0]} operand')
        return False
    for i in range(1, len(required_operands)):
        if len(args) < i + 2:
            print(f"{command}: missing {required_operands[i]} operand after '{args[i]}'")
            return False
    # Check if extra operands are present
    if len(args) - 1 > len(required_operands):
        print(f'{command}: extra operands are present, expected [{len(required_operands)}] - got [{len(args) - 1}]')
        return False
    return True


def request_datanodes(datanodes, command, method, data=None):
    resp = None
    for try_counter in range(MAX_REQUEST_COUNT):
        for datanode in datanodes:
            node_address = f"{datanode['ip']}:{datanode['port']}"
            if method == "GET":
                resp = requests.get(join(node_address, command), data=data)
            elif method == "POST":
                resp = requests.post(join(node_address, command), data=data)
            elif method == "DELETE":
                resp = requests.delete(join(node_address, command), data=data)
            if resp.status_code == 200:
                return Response(status=200)
    raise Exception


def os_read_file(path):
    try:
        return open(path, 'rb').read()
    except OSError as e:
        print(e)
        return None


def join_path(filename, destination):
    """
    Join destination dir and filename
    :param filename:
    :param destination:
    :return: joined absolute normalized (without loops) path
    """
    return make_abs(join(destination, filename))


def make_abs(path):
    """
    If path is not absolute, join with a pending working directory
    :param path:
    :return: absolute normalized (without loops) path
    """
    if isabs(path):
        return normpath(path)
    else:
        return normpath(join(pwd, path))
