import requests
import os

MASTER_NODE = "http://localhost:3030/"

if __name__ == "__main__":
    print("Client is working , but run Master Node first")
    while True:
        print("Enter the command(available are: -ping)")
        command = input("-")
        if command == "ping":
            resp = requests.get(os.path.join(MASTER_NODE, command))
            if resp.status_code == 200:
                print(resp.content)
            else:
                print("Error")
