import requests
import os

from client.client_utils import *

MASTER_NODE = "http://localhost:3030/"


def show_help(*unused):
    """Print out commands' description"""
    del unused
    print('Commands and arguments:\n'
          '$ ping                     : ping the filesystem\n'
          '$ init                     : initialize the storage\n'
          '$ mv <file> <destination>  : move file to a given destination dir\n'
          '$ put <file> <destination> : put a local file to the remote filesystem\n'
          '$ cd <destination>         : change remote pwd to a destination dir\n')


def ping_master_node(*unused):
    """Check that master_node is alive"""
    del unused
    resp = requests.get(os.path.join(MASTER_NODE, 'ping'))
    check_response(resp)


def initialize_filesystem(*unused):
    """Clear filesystem, prepare it for work"""
    del unused
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
    ]):
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
        data = read_file(filename)
        if data:
            destination = args[2]
            path = join_path(filename, destination)

            # Request to store a file in the filesystem
            # Request structure: /file?filename=<path>
            resp = requests.post(os.path.join(MASTER_NODE, f'file?filename={path}'))
            if check_response(resp):
                content = resp.json()
                nodes = content['datanodes']  # Available storage datanodes
                file = content['file']  # View of a file from the perspective of a masternode
                if data:
                    # Request to store a file in the storage
                    # Request structure: /file?filename=<filename>
                    request_datanodes(nodes, f'file?filename={file["file_id"]}', 'POST', data=data)


def change_dir(*args):
    """
    Change remote pwd to a destination folder
    :param args: cd <destination>
    :return:
    """
    if check_args('cd', args, required_operands=['destination']):
        destination = make_abs(args[1])
        resp = requests.get(os.path.join(MASTER_NODE, f'directory?name={destination}'))
        if check_response(resp):
            set_pwd(destination)


command_tree = {
    'help': show_help,
    'ping': ping_master_node,
    'init': initialize_filesystem,
    'mv': move_file,
    'put': put_file,
    'cd': change_dir,
}

if __name__ == "__main__":
    print("Client is working , but run Master Node first")
    while True:
        print("Enter the command(type '$ help' to view the commands' description)")
        args = input("$ ").split()
        if len(args) == 0:
            continue
        try:
            command_tree[args[0]](*args)
        except KeyError:
            print("No such command, please try again")
        except Exception:
            print("Command failed, please try again ")
