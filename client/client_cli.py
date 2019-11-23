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
          '-put <file> <destination> : put a local file to the remote filesystem\n'
          '-get <file> <local_destinatio> :put a remote file to local filesystem')


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
    ]):
        filename = make_abs(args[1])
        destination = make_abs(args[2])
        # Request to put a file to a new destination
        # Request structure: /file?filename=<name>&destination=<dest>
        resp = requests.put(os.path.join(MASTER_NODE, f'file?filename={filename}&destination={destination}'))
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
        data = os_read_file(filename)
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


def read_file(*args):
    if check_args('get', args, ['file', 'local_destination']):
        fpath = make_abs(args[1])
        dest = args[2]
        resp = requests.get(os.path.join(MASTER_NODE, f"file?filename={fpath}"))
        if resp.status_code != 200:
            print(resp.content.decode())
        else:
            data = resp.json()
            resp = request_datanodes(data['nodes'], f"file?filename={data['file_id']}", 'GET')
            if resp.status_code == 200:
                print(f"=============\nFile {fpath} successfully retrieved")
                print(f"Saving to {dest}")
                try:
                    f = open(dest, 'wb')
                    f.write(resp.content)
                    print("Successfully saved")
                except:
                    print("Error while saving on local filesystem")
            else:
                print(f"Error reading from dataNode", resp.content.decode())


command_tree = {
    'help': show_help,
    'ping': ping_master_node,
    'init': initialize_filesystem,
    'mv': move_file,
    'put': put_file,
    'get': read_file,
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
