import os

from client.client_utils import *

MASTER_NODE = "http://localhost:3030/"


def show_help(*unused):
    """Print out commands' description"""
    print(
        "Commands and arguments:\n"
        "$ ping                     : ping the filesystem\n"
        "$ init                     : initialize the storage\n"
        "$ mv <file> <destination>  : move file to a given destination dir\n"
        "$ put <file> <destination> : put a local file to the remote filesystem\n"
        "$ cd <destination>         : change remote pwd to a destination dir\n"
        "$ mkdir <directory>        : create a specified dir\n"
        "$ get <file> <destination> : put a remote file to a local filesystem\n"
        "$ ls [destination]         : list file information for a destination, no [destination] == '.'\n"
        "Note:\n<required_positional_operand>, [optional_operand]\n"
    )


def ping_master_node(*unused):
    """Check that master_node is alive"""
    resp = requests.get(os.path.join(MASTER_NODE, "ping"))
    check_response(resp)


def initialize_filesystem(*unused):
    """Clear filesystem, prepare it for work"""
    resp = requests.delete(os.path.join(MASTER_NODE, "filesystem"))
    check_response(resp)


def move_file(*args):
    """
    Move a file to a destination folder
    :param args: mv <file> <destination>
    :return:
    """
    if check_args("mv", args, required_operands=["file", "destination"]):
        filename = make_abs(args[1])
        destination = make_abs(args[2])
        # Request to put a file to a new destination
        # Request structure: /file?filename=<name>&destination=<dest>
        resp = requests.put(
            os.path.join(
                MASTER_NODE, f"file?filename={filename}&destination={destination}"
            )
        )
        check_response(resp)


def put_file(*args):
    """
    Put local file to a remote destination folder
    :param args: mv <file> <destination>
    :return:
    """
    if check_args("put", args, required_operands=["file", "destination"]):
        filename = args[1]
        data = os_read_file(filename)
        if data:
            destination = args[2]
            path = join_path(filename, destination)

            # Request to store a file in the filesystem
            # Request structure: /file?filename=<path>
            resp = requests.post(os.path.join(MASTER_NODE, f"file?filename={path}"))
            if check_response(resp, pretty_print_enabled=True):
                content = resp.json()
                nodes = content["datanodes"]  # Available storage datanodes
                file = content[
                    "file"
                ]  # View of a file from the perspective of a masternode
                if data:
                    # Request to store a file in the storage
                    # Request structure: /file?filename=<filename>
                    request_datanodes(
                        nodes, f'file?filename={file["file_id"]}', "POST", data=data
                    )


def change_dir(*args):
    """
    Change remote pwd to a destination folder
    :param args: cd <destination>
    :return:
    """
    if check_args("cd", args, required_operands=["destination"]):
        destination = make_abs(args[1])
        resp = requests.get(os.path.join(MASTER_NODE, f"directory?name={destination}"))
        if check_response(resp, pretty_print_enabled=True):
            set_pwd(destination)


def make_dir(*args):
    """
    Change remote pwd to a destination folder
    :param args: mkdir <destination>
    :return:
    """
    if check_args("mkdir", args, required_operands=["destination"]):
        destination = make_abs(args[1])
        resp = requests.post(os.path.join(MASTER_NODE, f"directory?name={destination}"))
        check_response(resp)


def read_file(*args):
    if check_args("get", args, ["file", "local_destination"]):
        fpath = make_abs(args[1])
        dest = args[2]
        resp = requests.get(os.path.join(MASTER_NODE, f"file?filename={fpath}"))
        if check_response(resp):
            data = resp.json()
            resp = request_datanodes(
                data["nodes"], f"file?filename={data['file_id']}", "GET"
            )
            if check_response(resp):
                print(f"=============\nFile {fpath} successfully retrieved")
                print(f"Saving to {dest}")
                try:
                    f = open(dest, "wb")
                    f.write(resp.content)
                    print("Successfully saved")
                except:
                    print("Error while saving on local filesystem")


def remove_file_or_dir(*args):
    if check_args("rm", args, ["file_or_dir"]):
        fpath = make_abs(args[1])
        pass
        # get directory info {files:[], dirs:[]} via get /directory?name={}
        # if file call on master delete /file?filename
        # if dir 1) check if not empty via get /directory?name={} 2)
        # call delete /directory?name={}


def list_dir(*args):
    """
    List file information
    :param args: ls [destination]
    :return:
    """
    if check_args("ls", args, optional_operands=["destination"]):
        if len(args) > 1:
            destination = make_abs(args[1])
        else:
            destination = make_abs(".")

        resp = requests.get(os.path.join(MASTER_NODE, f"directory?name={destination}"))
        if not check_response(resp, pretty_print_enabled=True):
            resp = requests.get(os.path.join(MASTER_NODE, f"file?filename={destination}"))
            if not check_response(resp, pretty_print_enabled=True):
                print(f"ls: cannot access '{destination}': No such file or directory")


command_tree = {
    "help": show_help,
    "ping": ping_master_node,
    "init": initialize_filesystem,
    "mv": move_file,
    "put": put_file,
    "cd": change_dir,
    "mkdir": make_dir,
    "get": read_file,
    "rm": remove_file_or_dir,
    "ls": list_dir,
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
            print(f"No such command '{args[0]}', please try again")
        except Exception:
            print("Command failed, please try again ")
