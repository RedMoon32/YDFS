import requests
import json
from pandas.io.json import json_normalize
from os.path import isabs, join, normpath

MAX_REQUEST_COUNT = 3
pwd = "/"


def pretty_print(data):
    try:
        data = json.loads(data)
        for d in data:
            if d == "dirs":
                print("dirs :", data[d])
            else:
                json_data = json_normalize(data[d])
                if not json_data.empty:
                    print(d, ":")
                    print(json_data)
                else:
                    print(d, ":", [])
    except Exception as e:
        print("Response JSON parse error")


def check_response(resp, pretty_print_enabled=False):
    if resp.status_code // 100 == 2:  # status codes 2xx
        if pretty_print_enabled:
            pretty_print(resp.content.decode())
        else:
            print(resp.content.decode())
        return True
    else:
        if not pretty_print_enabled:
            print("Error:", resp.content.decode())
        return False


def check_args(command: str, args: tuple, required_operands=None, optional_operands=None):
    """
    Check that number of arguments is correct.
    :param command: CLI command to check. Used for user prompt.
    :param args: received arguments
    :param required_operands: obligatory positional command operands
    :param optional_operands: optional command operands, must go after all required operands
    :type: list
    :return:
    """
    if required_operands is None:
        required_operands = []
    if optional_operands is None:
        optional_operands = []
    if len(required_operands) > 0:
        if len(args) < 2:
            print(f"{command}: missing {required_operands[0]} operand")
            return False
        for i in range(1, len(required_operands)):
            if len(args) < i + 2:
                print(
                    f"{command}: missing {required_operands[i]} operand after '{args[i]}'"
                )
                return False
    # Check if extra operands are present
    expected_count = len(required_operands) + len(optional_operands)
    if len(args) - 1 > expected_count:
        print(
            f"{command}: extra operands are present, expected [{expected_count}] - got [{len(args) - 1}]"
        )
        return False
    return True


def request_datanodes(datanodes, command, method, data=None):
    resp = None
    for try_counter in range(MAX_REQUEST_COUNT):
        try:
            for datanode in datanodes:
                node_address = f"{datanode['ip']}:{datanode['port']}"
                if method == "GET":
                    resp = requests.get(join(node_address, command), data=data)
                elif method == "POST":
                    resp = requests.post(join(node_address, command), data=data)
                elif method == "DELETE":
                    resp = requests.delete(join(node_address, command), data=data)
                return resp
        except Exception:
            pass
    print("Error reading from data-nodes")


def os_read_file(path):
    try:
        return open(path, "rb").read()
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


def set_pwd(path):
    """Setter for a pending working directory"""
    global pwd
    pwd = path
