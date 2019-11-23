import requests
import os

from client.client_utils import *

MASTER_NODE = "http://localhost:3030/"


def show_help(*args):
    """Print out commands' description"""
    print('Commands and arguments:\n'
          '-ping                     : ping the filesystem\n'
          '-init                     : initialize the storage\n'
          '-mv <file> <destination>  : move file to a given destination dir\n'
          '-put <file> <destination> : put a local file to the remote filesystem\n')


def ping_master_node(*args):
    """Check that master_node is alive"""
    resp = requests.get(os.path.join(MASTER_NODE, 'ping'))
    check_response(resp)


def initialize_filesystem(*args):
    """Clear filesystem, prepare it for work"""
    resp = requests.delete(os.path.join(MASTER_NODE, 'filesystem'))
    check_response(resp)


def move_file(*args):
    """
    Move a file to a destination folder
    :param args: mv <file> <destination>
    :return:
    """
    if check_args('mv', args, required_operands=[
        'file',
        'destination'
    ]) == 0:
        # Request to put a file to a new destination
        # Request structure: /file?filename=<name>&destination=<dest>
        resp = requests.put(os.path.join(MASTER_NODE, f'file?filename={args[1]}&destination={make_abs(args[2])}'))
        check_response(resp)


def put_file(*args):
    """
    Put local file to a remote destination folder
    :param args: mv <file> <destination>
    :return:
    """
    if check_args('put', args, required_operands=[
        'file',
        'destination'
    ]):
        filename = args[1]
        destination = args[2]
        path = join_path(filename, destination)

        # Request to store a file in the filesystem
        # Request structure: /file?filename=<path>
        resp = requests.post(os.path.join(MASTER_NODE, f'file?filename={path}'))
        data = os_read_file(filename)
        if check_response(resp) == 0:
            content = resp.json()
            nodes = content['datanodes']  # Available storage datanodes
            file = content['file']  # View of a file from the perspective of a masternode
            if data:
                # Request to store a file in the storage
                # Request structure: /file?filename=<filename>
                request_datanodes(nodes, f'file?filename={file["file_id"]}', 'POST', data=data)


def read_file(*args):
    if check_args('cat', args, ['file']):
        fpath = make_abs(args[1])
        resp = requests.get(os.path.join(MASTER_NODE, f"file?name={fpath}"))
        if resp.status_code == 404:
            print("File not found")
        else:
            data = resp.json()
            resp = request_datanodes(data['nodes'], f"file?filename={data['file_id']}", 'GET')
            if resp.status_code == 200:
                print(f"=============\nFile {fpath} content:\n\n{resp.content.decode()}")
            else:
                print(f"Error reading from dataNode", resp.content.decode())


command_tree = {
    'help': show_help,
    'ping': ping_master_node,
    'init': initialize_filesystem,
    'mv': move_file,
    'put': put_file,
    'cat': read_file,
}

if __name__ == "__main__":
    print("Client is working , but run Master Node first")
    while True:
        print("Enter the command(type -help to view commands' description)")
        args = input("$ ").split()
        if len(args) == 0:
            continue
        try:
            command_tree[args[0]](*args)
        except KeyError:
            print("No such command, please try again")
        except Exception:
            print("Command failed, please try again ")
