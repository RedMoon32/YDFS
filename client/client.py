import requests
import os

MASTER_NODE = "http://localhost:3030/"


def check_response(resp):
    if resp.status_code == 200:
        print(resp.content.decode())
    else:
        print("Error:", resp.content.decode())


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
    if len(args) < 2:
        print('mv: missing file operand')
        return
    elif len(args) < 3:
        print(f"mv: missing destination file operand after '{args[1]}'")
        return
    # Request to put a file to a new destination
    # Request structure: /file?op=<operation>&filename=<name>&destination=<dest>
    resp = requests.put(os.path.join(MASTER_NODE, f'file?op=mv&filename={args[1]}&destination={args[2]}'))
    check_response(resp)


# Copy a file to a destination folder
# -mv <source_file> <target_file>
def copy_file(args):
    if len(args) != 3:
        print("Wrong arguments, copy command should be in these format:\n "
              "\t-mv <source_file> <target_file>")
        return
    # Request to put a file to a new destination
    # Request structure: /file?op=<operation>&source=<source_file>&target=<target_file>
    resp = requests.put(os.path.join(MASTER_NODE, f'file?operation=cp&filename={args[1]}&target={args[2]}'))
    check_response(resp)

if __name__ == "__main__":
    print("Client is working , but run Master Node first")
    while True:
        print("Enter the command(type -help to view commands' description)")
        args = input("-").split()
        if args[0] == 'help':
            show_help()
        elif args[0] == "ping":
            ping_master_node()
        elif args[0] == 'init':
            initialize_filesystem()
        elif args[0] == 'mv':
            move_file(args)
        elif args[0] == 'cp':
            copy_file(args)
