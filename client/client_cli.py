import requests
import os
import json

from client.client_utils import check_args, check_response, request_datanodes, read_file

MASTER_NODE = "http://localhost:3030/"


# Print out commands' description
# -help
def show_help():
    print('Commands and arguments:\n'
          '-ping                    : ping the filesystem\n'
          '-init                    : initialize the storage\n'
          '-mv <file> <destination> : move file to a given destination dir\n')


# Check that master_node is alive
# -ping
def ping_master_node():
    resp = requests.get(os.path.join(MASTER_NODE, args[0]))
    check_response(resp)


# Clear filesystem, prepare it for work
# -init
def initialize_filesystem():
    resp = requests.delete(os.path.join(MASTER_NODE, 'filesystem'))
    check_response(resp)


# Move a file to a destination folder
# -mv <file> <destination>
def move_file(args):
    if check_args('mv', args, missing_operands=[
        'file',
        'destination'
    ]) == 0:
        # Request to put a file to a new destination
        # Request structure: /file?filename=<name>&destination=<dest>
        resp = requests.put(os.path.join(MASTER_NODE, f'file?filename={args[1]}&destination={args[2]}'))
        check_response(resp)


# Put local file to a remote destination folder
# -put <file> <destination>
def put_file(args):
    if check_args('put', args, missing_operands=[
        'file',
        'destination'
    ]) == 0:
        # Request to store a file in the filesystem
        # Request structure: /file?filename=<name>&destination=<dest>
        filename = args[1]
        destination = args[2]
        resp = requests.post(os.path.join(MASTER_NODE, f'file?filename={filename}&destination={destination}'))
        content = resp.json()
        nodes = content['datanodes']  # Available storage datanodes
        file = content['file']        # View of a file from the perspective of a masternode
        if check_response(resp) == 0:
            data = read_file(filename)
            if data:
                # Request to store a file in the storage
                # Request structure: /file?filename=<name>&destination=<dest>
                request_datanodes(nodes, f'file?filename={file["file_id"]}&destination={destination}', 'POST', data=data)


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
