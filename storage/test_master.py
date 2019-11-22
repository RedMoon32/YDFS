import pytest

from storage.master_node import app as master_node, DataNode
import storage.master_node

from storage.file_system import FileSystem, File


@pytest.fixture
def client():
    master_node.config['TESTING'] = True

    with master_node.test_client() as client:
        yield client


def clean():
    storage.master_node.fs = FileSystem()
    storage.master_node.data_nodes = []


def test_register_datanode(client):
    resp = client.post("/datanode?ip=101.101.101.101&port=2000")
    assert resp.status_code == 201

    resp = client.post("/datanode?ip=101.101.101.101&port=2000")
    assert resp.status_code == 400

    clean()


def test_file_location_to_store(client):
    resp = client.post("/datanode?ip=101.101.101.101&port=2000")
    assert resp.status_code == 201

    resp = client.post("/file?filename=a.txt")
    assert resp.json['datanodes'] == [{"ip": "101.101.101.101",
                                       "port": 2000}]

    resp = client.post("/file?filename=/b/a.txt")
    assert resp.status_code == 400

    storage.master_node.fs._dirs.append("/b")

    resp = client.post("/file?filename=/b/a.txt")
    assert resp.status_code == 200

    clean()


def test_file_node_locations(client):
    clean()

    storage.master_node.fs._file_mapper = {
        "file1": File("file1", 1, [DataNode("127.0.0.1", "333"), DataNode("127.0.0.2", "333")],
                      {}),
    }
    resp = client.get("/file?filename=file1")
    assert resp.json['nodes'] == [{"ip": "127.0.0.1",
                                   "port": 333},
                                  {"ip": "127.0.0.2",
                                   "port": 333}]


def test_mkdir(client):
    clean()

    resp = client.post("/directory?name=/b")
    assert resp.status_code == 201

    resp = client.post("/directory?name=c/d")
    assert resp.status_code == 400

    resp = client.post("/directory?name=c")
    assert resp.status_code == 201

    resp = client.post("/directory?name=/c")
    assert resp.status_code == 400

    resp = client.post("/directory?name=c/d")
    assert resp.status_code == 201

    assert storage.master_node.fs.dir_exists("/b")
    assert storage.master_node.fs.dir_exists("/c")
    assert storage.master_node.fs.dir_exists("/c/d")


def test_get_directory(client):
    clean()

    storage.master_node.fs._dirs = ["/", "/a", "/b", "/a/c"]
    storage.master_node.fs._file_mapper = {
        "/a/a1.txt": File("1", 111, [DataNode("127.0.0.1", "333"), DataNode("127.0.0.2", "333")],
                          {}),
        "/a/c/a2.txt": File("2", 222, [DataNode("127.0.0.1", "333"), DataNode("127.0.0.2", "333")],
                            {}),
        "/r1.txt": File("3", 333, [DataNode("127.0.0.1", "333"), DataNode("127.0.0.2", "333")],
                        {}),
        "/r2.txt": File("4", 444, [DataNode("127.0.0.1", "333"), DataNode("127.0.0.2", "333")],
                        {}),
    }

    resp = client.get("directory?name=/a/c")
    assert resp.json["dirs"] == []
    assert resp.json["files"][0]["file_id"] == 222

    resp = client.get("directory?name=/")
    assert resp.json["dirs"] == ["/a", "/b"] and len(resp.json["files"]) == 2
    assert resp.json["files"][0]["file_id"] == 333
    assert resp.json["files"][1]["file_id"] == 444


def test_file_move(client):
    clean()

    storage.master_node.fs.add_file("a.txt")
    storage.master_node.fs._dirs.append("/b")

    resp = client.put("/file?filename=a.txt&destination=/b")
    assert resp.status_code == 200
    assert storage.master_node.fs.get_file("a.txt") is None
    assert storage.master_node.fs.get_file("/b/a.txt") is not None

    resp = client.put("/file?filename=/b/a.txt&destination=/b/c")
    assert resp.status_code == 404

    storage.master_node.fs._dirs.append("/c")
    storage.master_node.fs.add_file("/c/a.txt")

    resp = client.put("/file?filename=/b/a.txt&destination=/c")
    assert resp.status_code == 400


def test_filesystem_delete(client):
    client.delete('filesystem')
    assert storage.master_node.fs._id == 0
    assert storage.master_node.fs._dirs == ['/']
    assert storage.master_node.fs._file_mapper == {}
