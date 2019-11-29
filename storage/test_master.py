import pook
import pytest
from unittest.mock import Mock
from storage.master_node import app as master_node, DataNode
import storage.master_node

from storage.file_system import FileSystem, File


@pytest.fixture
def client():
    master_node.config["TESTING"] = True

    with master_node.test_client() as client:
        yield client


def clean():
    storage.master_node.fs = FileSystem()
    storage.master_node.data_nodes = []
    storage.master_node.fs._file_mapper = {}
    storage.master_node.fs._file_id_maper = {}


def test_register_datanode(client):
    resp = client.post("/datanode?port=2000")
    assert resp.status_code == 201

    resp = client.post("/datanode?port=2000")
    assert resp.status_code == 400

    clean()


def test_file_location_to_store(client):
    storage.master_node.data_nodes = [DataNode("101.101.101.101", 2000)]

    resp = client.post("/file?filename=a.txt")
    assert resp.json["datanodes"] == [{"ip": "101.101.101.101", "port": 2000}]
    assert "a.txt" in storage.master_node.fs._file_mapper

    resp = client.post("/file?filename=/b/a.txt")
    assert resp.status_code == 400

    storage.master_node.fs._dirs.append("/b")

    resp = client.post("/file?filename=/b/a.txt")
    assert resp.status_code == 200
    assert "/b/a.txt" in storage.master_node.fs._file_mapper

    clean()


def test_file_node_locations(client):
    clean()

    storage.master_node.fs._file_mapper = {
        "file1": File(
            "file1", 1, [DataNode("127.0.0.1", "333"), DataNode("127.0.0.2", "333")], {}
        ),
    }
    resp = client.get("/file?filename=file1")
    assert resp.json["file"]["nodes"] == [
        {"ip": "127.0.0.1", "port": 333},
        {"ip": "127.0.0.2", "port": 333},
    ]


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
        "/a/a1.txt": File(
            "1", 111, [DataNode("127.0.0.1", "333"), DataNode("127.0.0.2", "333")], {}
        ),
        "/a/c/a2.txt": File(
            "2", 222, [DataNode("127.0.0.1", "333"), DataNode("127.0.0.2", "333")], {}
        ),
        "/r1.txt": File(
            "3", 333, [DataNode("127.0.0.1", "333"), DataNode("127.0.0.2", "333")], {}
        ),
        "/r2.txt": File(
            "4", 444, [DataNode("127.0.0.1", "333"), DataNode("127.0.0.2", "333")], {}
        ),
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
    clean()

    client.delete("filesystem")
    assert storage.master_node.fs._id == 0
    assert storage.master_node.fs._dirs == ["/"]
    assert storage.master_node.fs._file_mapper == {}


@pook.on
def test_delete_file(client):
    clean()

    storage.master_node.fs._dirs = ["/", "/a", "/b", "/a/c"]
    storage.master_node.fs._file_mapper = {
        "/a/a1.txt": File("/a/a1.txt", 0, [DataNode("http://127.0.0.1", "333")], {}),
        "/a/a2.txt": File("/a/a2.txt", 1, [DataNode("http://127.0.0.1", "333")], {}),
        "/a/c/a2.txt": File(
            "/a/c/a2.txt", 2, [DataNode("http://127.0.0.1", "333")], {}
        ),
        "/r1.txt": File("/r1.txt", 3, [DataNode("http://127.0.0.1", "333")], {}),
        "/r2.txt": File("/r2.txt", 4, [DataNode("http://127.0.0.1", "333")], {}),
    }

    fid_mapper = {0: None, 1: None, 2: None, 3: None, 4: None}
    storage.master_node.fs._file_id_mapper = fid_mapper

    mocks = []
    for fid in range(1, 5):
        mocks.append(
            pook.delete(f"http://127.0.0.1:333/file?filename={fid}", reply=200)
        )
    resp = client.delete("/file?filename=/a/a2.txt")
    assert resp.status_code == 200
    assert not storage.master_node.fs.file_exists("/a/a2.txt")

    resp = client.delete("/file?filename=/a/a2.txt")
    assert resp.status_code == 404

    resp = client.delete("/file?filename=/r1.txt")
    assert resp.status_code == 200

    resp = client.delete("/file?filename=/r2.txt")
    assert resp.status_code == 200

    correct = {
        "/a/a1.txt": File("/a/a1.txt", 0, [DataNode("http://127.0.0.1", "333")], {}),
        "/a/c/a2.txt": File(
            "/a/c/a2.txt", 2, [DataNode("http://127.0.0.1", "333")], {}
        ),
    }

    assert len(storage.master_node.fs._file_mapper.keys()) == 2
    assert correct["/a/a1.txt"] == storage.master_node.fs.get_file("/a/a1.txt")
    assert correct["/a/c/a2.txt"] == storage.master_node.fs.get_file("/a/c/a2.txt")
    assert 0 in fid_mapper and 2 in fid_mapper


@pook.on
def test_delete_dir(client):
    clean()

    # we will delete /a directory and all its subdirectories/folders
    storage.master_node.fs._dirs = ["/", "/a", "/b", "/a/c", "/a/c/d", "/a/c/d/e"]
    storage.master_node.fs._file_mapper = {
        "/a/a1.txt": File("/a/a1.txt", 1, [DataNode("http://127.0.0.1", "333")], {}),
        "/a/a2.txt": File("/a/a2.txt", 1, [DataNode("http://127.0.0.1", "333")], {}),
        "/a/c/d/b.txt": File(
            "/a/c/d/b.txt", 1, [DataNode("http://127.0.0.1", "333")], {}
        ),
        "/a/c/d/e/b.txt": File(
            "/a/c/d/e/b.txt", 1, [DataNode("http://127.0.0.1", "333")], {}
        ),
        "/a/c/d/c.txt": File(
            "/a/c/d/c.txt", 1, [DataNode("http://127.0.0.1", "333")], {}
        ),
        "/a/c/a2.txt": File(
            "/a/c/a2.txt", 1, [DataNode("http://127.0.0.1", "333")], {}
        ),
        "/b/a2.txt": File("/b/a2.txt", 1, [DataNode("http://127.0.0.1", "333")], {}),
        "/r1.txt": File("/r1.txt", 1, [DataNode("http://127.0.0.1", "333")], {}),
        "/r2.txt": File("/r2.txt", 1, [DataNode("http://127.0.0.1", "333")], {}),
    }
    mock = pook.delete("http://127.0.0.1:333/file?filename=1", reply=200, times=20)
    storage.master_node.fs._file_id_mapper = Mock()
    resp = client.delete("/directory?name=/a")
    assert resp.status_code == 200
    assert storage.master_node.fs._dirs == ["/", "/b"]

    newfs = storage.master_node.fs._file_mapper.keys()
    assert len(newfs) == 3
    assert "/r1.txt" in newfs
    assert "/r2.txt" in newfs
    assert "/b/a2.txt" in newfs
    assert mock.calls == 6

    clean()
