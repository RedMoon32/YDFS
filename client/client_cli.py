import requests
import os

from client.client_utils import *

MASTER_NODE = "http://localhost:3030/"


def show_help():
    """Print out commands' description"""
    print('Commands and arguments:\n'
          '-ping                     : ping the filesystem\n'
          '-init                     : initialize the storage\n'
          '-mv <file> <destination>  : move file to a given destination dir\n'
          '-put <file> <destination> : put a local file to the remote filesystem\n')


def ping_master_node():
    """Check that master_node is alive"""
    resp = requests.get(os.path.join(MASTER_NODE, args[0]))
    check_response(resp)


def initialize_filesystem():
    """Clear filesystem, prepare it for work"""
    resp = requests.delete(os.path.join(MASTER_NODE, 'filesystem'))
    check_response(resp)


def move_file(args: list):
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


def put_file(args: list):
    """
    Put local file to a remote destination folder
    :param args: mv <file> <destination>
    :return:
    """
    if check_args('put', args, required_operands=[
        'file',
        'destination'
    ]) == 0:
        filename = args[1]
        destination = args[2]
        path = join_path(filename, destination)

        # Request to store a file in the filesystem
        # Request structure: /file?filename=<path>
        resp = requests.post(os.path.join(MASTER_NODE, f'file?filename={path}'))
        if check_response(resp) == 0:
            content = resp.json()
            nodes = content['datanodes']  # Available storage datanodes
            file = content['file']        # View of a file from the perspective of a masternode
            data = read_file(filename)
            if data:
                # Request to store a file in the storage
                # Request structure: /file?filename=<filename>
                request_datanodes(nodes, f'file?filename={file["file_id"]}', 'POST', data=data)


if __name__ == "__main__":
    print("Client is working , but run Master Node first")
    while True:
        print("Enter the command(type -help to view commands' description)")
        args = input("-").split()
        if len(args) == 0:
            continue
        elif args[0] == 'help':
            show_help()
        elif args[0] == "ping":
            ping_master_node()
        elif args[0] == 'init':
            initialize_filesystem()
        elif args[0] == 'mv':
            move_file(args)
        elif args[0] == 'put':
            put_file(args)
