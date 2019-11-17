import requests
import os

MASTER_NODE = "http://localhost:3030/"

if __name__ == "__main__":
    print("Client is working , but run Master Node first")
    while True:
        print("Enter the command(type -help to view commands' description)")
        args = input("-").split()
        if args[0] == 'help':
            print('Commands and arguments:\n'
                  '-ping                    : ping the filesystem\n'
                  '-init                    : initialize the storage\n'
                  '-mv <file> <destination> : move file to a given destination dir\n')
        elif args[0] == "ping":
            resp = requests.get(os.path.join(MASTER_NODE, args[0]))
            if resp.status_code == 200:
                print(resp.content.decode())
            else:
                print("Error:", resp.content.decode())
        elif args[0] == 'init':
            resp = requests.delete(os.path.join(MASTER_NODE, 'filesystem'))
            if resp.status_code == 200:
                print(resp.content.decode())
            else:
                print("Error:", resp.content.decode())
        elif args[0] == 'mv':
            if len(args) < 2:
                print('mv: missing file operand')
                continue
            elif len(args) < 3:
                print(f"mv: missing destination file operand after '{args[1]}'")
                continue
            resp = requests.put(os.path.join(MASTER_NODE, f'file?filename={args[1]}&destination={args[2]}'))
            if resp.status_code == 200:
                print(resp.content.decode())
            else:
                print("Error:", resp.content.decode())
