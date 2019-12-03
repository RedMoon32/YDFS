import os

from client.client_utils import *

MASTER_NODE = os.getenv("MASTER_NODE", "http://localhost:3030/")


def show_help(*unused):
    """Print out commands' description"""
    print(
        "Commands and arguments:\n"
        "ping                     : ping the filesystem\n"
        "init                     : initialize the storage\n"
        "status                   : view status of the filesystem\n"
        "mv <file> <destination>  : move a file to a given destination dir\n"
        "cp <file> <target>       : move a file to a given target path containing a new filename\n"
        "put <file> <destination> : put a local file to the remote filesystem\n"
        "cd <destination>         : change remote pwd to a destination dir\n"
        "mkdir <directory>        : create a specified dir\n"
        "get <file> <destination> : put a remote file to a local filesystem\n"
        "ls [destination]         : list file information for a destination, no [destination] == '.'\n"
        "rm <destination>         : remove a destination directory or a file\n"
        "Note:\n<required_positional_operand>, [optional_operand]\n"
    )


def ping_master_node(*unused):
    """Check that master_node is alive"""
    resp = requests.get(os.path.join(MASTER_NODE, "ping"))
    check_response(resp, "ping")


def status(*unused):
    """Check master_node status"""
    resp = requests.get(os.path.join(MASTER_NODE, "status"))
    check_response(resp, "status", pretty_print_enabled=True)


def initialize_filesystem(*unused):
    """Clear filesystem, prepare it for work"""
    resp = requests.delete(os.path.join(MASTER_NODE, "filesystem"))
    check_response(resp, "init")


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
        check_response(resp, "mv")


def copy_file(*args):
    """
    Copy a file to a target path
    :param args: mv <file> <target>
    :return:
    """
    if check_args("cp", args, required_operands=["file", "target_path"]):
        fpath = make_abs(args[1])
        resp = requests.get(os.path.join(MASTER_NODE, f"file?filename={fpath}"))
        if check_response(resp, "get", print_content=False):
            data = resp.json()
            resp = request_datanodes(
                data["file"]["nodes"], f"file?filename={data['file']['file_id']}", "GET"
            )
            if check_response(resp, "get", print_content=False):
                data = resp.content
                path = make_abs(args[2])

                # Request to store a file in the filesystem
                # Request structure: /file?filename=<path>
                resp = requests.post(os.path.join(MASTER_NODE, f"file?filename={path}"))
                if check_response(resp, "cp", print_content=False):
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


def put_file(*args):
    """
    Put local file to a remote destination folder
    :param args: mv <file> <destination>
    :return:
    """
    if check_args("put", args, required_operands=["file", "destination"]):
        fpath = args[1]
        data = os_read_file(fpath)
        if data:
            destination = make_abs(args[2])
            filename = os.path.basename(fpath)
            path = join_path(filename, destination)

            # Request to store a file in the filesystem
            # Request structure: /file?filename=<path>
            resp = requests.post(os.path.join(MASTER_NODE, f"file?filename={path}"))
            if check_response(resp, "put", print_content=False):
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
        if check_response(resp, "cd", verbose=False):
            set_pwd(destination)
        else:
            print(f"cd: {destination}: No such file or directory")


def make_dir(*args):
    """
    Change remote pwd to a destination folder
    :param args: mkdir <destination>
    :return:
    """
    if check_args("mkdir", args, required_operands=["destination"]):
        destination = make_abs(args[1])
        resp = requests.post(os.path.join(MASTER_NODE, f"directory?name={destination}"))
        check_response(resp, "mkdir")


def read_file(*args):
    if check_args("get", args, ["file", "local_destination"]):
        fpath = make_abs(args[1])
        dest = args[2]
        resp = requests.get(os.path.join(MASTER_NODE, f"file?filename={fpath}"))
        if check_response(resp, "get", print_content=False):
            data = resp.json()
            resp = request_datanodes(
                data["file"]["nodes"], f"file?filename={data['file']['file_id']}", "GET"
            )
            if check_response(resp, "get", print_content=False):
                print(f"File '{fpath}' successfully retrieved")
                print(f"Saving to '{dest}'")
                try:
                    f = open(dest, "wb")
                    f.write(resp.content)
                    print("Successfully saved")
                except OSError as e:
                    print(
                        f"Error while saving on local filesystem: {e.strerror} '{e.filename}'"
                    )


def remove_file_or_dir(*args):
    """
    Remove a destination directory or a file
    :param args: rm <destination>
    :return:
    """
    if check_args("rm", args, ["file_or_dir"]):
        destination = make_abs(args[1])

        if get_pwd().startswith(destination):
            print(
                f"rm: cannot remove '{destination}': It is a prefix of the current working directory"
            )
            return

        dir_resp = requests.get(
            os.path.join(MASTER_NODE, f"directory?name={destination}")
        )
        file_resp = requests.get(
            os.path.join(MASTER_NODE, f"file?filename={destination}")
        )

        if check_response(file_resp, verbose=False):
            resp = requests.delete(
                os.path.join(MASTER_NODE, f"file?filename={destination}")
            )
            check_response(resp, "rm")
        elif check_response(dir_resp, verbose=False):
            data = dir_resp.json()
            if (
                    len(data["dirs"]) > 0 or len(data["files"]) > 0
            ):  # If destination directory is not empty
                # Prompt for yes/no while not get satisfactory answer
                while True:
                    inp = input(
                        f"rm: directory '{destination} is not empty, remove? [y/N]': "
                    ).split()
                    if check_args("rm", tuple(inp), optional_operands=["yes/no"]):
                        if (
                                len(inp) == 0 or inp[0].lower() == "n"
                        ):  # Consider as decline
                            break
                        ans = inp[0]
                        if ans.lower() == "y":  # Consider as accept
                            resp = requests.delete(
                                os.path.join(
                                    MASTER_NODE, f"directory?name={destination}"
                                )
                            )
                            check_response(resp, "rm")
                            break
                        else:
                            print("Incorrect input")
                            continue
            else:
                resp = requests.delete(
                    os.path.join(MASTER_NODE, f"directory?name={destination}")
                )
                check_response(resp, "rm")
        else:
            print(f"rm: cannot remove '{destination}': No such file or directory")


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
        if not check_response(resp, "ls", pretty_print_enabled=True):
            resp = requests.get(
                os.path.join(MASTER_NODE, f"file?filename={destination}")
            )
            if not check_response(resp, "ls", pretty_print_enabled=True):
                print(f"ls: cannot access '{destination}': No such file or directory")


command_tree = {
    "help": show_help,
    "ping": ping_master_node,
    "status": status,
    "init": initialize_filesystem,
    "mv": move_file,
    "cp": copy_file,
    "put": put_file,
    "cd": change_dir,
    "mkdir": make_dir,
    "get": read_file,
    "rm": remove_file_or_dir,
    "ls": list_dir,
}

if __name__ == "__main__":
    print("Client is working, but run Master Node first")
    print("Enter a command(type '$ help' to view the commands' description)")
    while True:
        args = input(get_pwd() + "$ ").split()
        if len(args) == 0:
            continue
        try:
            command_tree[args[0]](*args)
        except KeyError:
            print(f"No such command '{args[0]}', please try again")
        except Exception:
            print("Command failed, please try again")
