from client_cli import *
from client_utils import *


def create(path):
    MASTER_NODE = "http://3.14.131.38:3030/"
    resp = requests.post(os.path.join(MASTER_NODE, f"file?filename={path}"))
    if check_response(resp, "put", print_content=False):
        content = resp.json()
        nodes = content["datanodes"]  # Available storage datanodes
        file = content[
            "file"
        ]  # View of a file from the perspective of a masternode
        data = "aaaaaaviofdviofdvofdnbiofdnfdinbiofdnboifbniofdnbiofdniofnbiofdbn"
        if data:
            # Request to store a file in the storage
            # Request structure: /file?filename=<filename>
            request_datanodes(
                nodes, f'file?filename={file["file_id"]}', "POST", data=data
            )


if __name__ == "__main__":
    for i in range(1000):
        print(i)
        create("/" + str(i))
