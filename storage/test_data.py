import pook
import pytest
import os

from shutil import rmtree
from storage.data_node import app as data_node, FILE_STORE


@pytest.fixture
def client():
    data_node.config["TESTING"] = True

    with data_node.test_client() as client:
        yield client


def init_storage_test():
    rmtree(FILE_STORE)
    os.mkdir(FILE_STORE)


def test_filesystem_delete(client):
    init_storage_test()

    os.mkdir(os.path.join(FILE_STORE, "test"))
    assert len(os.listdir(FILE_STORE)) == 1
    client.delete("filesystem")
    assert os.path.exists(FILE_STORE)
    assert len(os.listdir(FILE_STORE)) == 0


def test_file_get(client):
    init_storage_test()

    fpath = os.path.join(FILE_STORE, "1")
    f = open(fpath, "w")
    data = "AAAAAAAAAABBBBBBBBBBBCCCCCCCCCCCCCCCCCCC"
    f.write(data)
    f.close()

    resp = client.get("/file?filename=1")
    assert resp.status_code == 200
    assert resp.data.decode("utf-8") == data

    resp = client.get("/file?filename=2")
    assert resp.status_code == 404

    os.remove(fpath)


@pook.on
def test_file_put(client):
    init_storage_test()
    mock = pook.post(
        "http://localhost:3030/file_created?file_id=1&port=2020", reply=200
    )

    assert not os.path.exists(os.path.join(FILE_STORE, "1"))
    data = "AAAAAAAAAAABBBBBBBBBBCCCCCCCCCCCCCC"

    resp = client.post("/file?filename=1", data=data)
    assert resp.status_code == 201

    resp = client.post("/file?filename=../b.txt", data=data)
    assert resp.status_code == 400

    resp = client.post("/file?filename=1", data=data)
    assert resp.status_code == 400


def test_file_delete(client):
    init_storage_test()
    fpath = os.path.join(FILE_STORE, "1")
    open(fpath, "w")

    assert client.get("/file?filename=1").status_code == 200

    assert client.delete("/file?filename=1").status_code == 200
    assert not os.path.exists(fpath)


def test_file_copy(client):
    init_storage_test()
    data = "AAAAAAAAAAABBBBBBBBBBCCCCCCCCCCCCCC"
    client.post("/file?filename=1", data=data)

    resp = client.put("/file?filename=1&target=2")
    assert resp.status_code == 201

    resp = client.put("/file?filename=1&target=2")
    assert resp.status_code == 400

    resp = client.put("/file?filename=3&target=4")
    assert resp.status_code == 404

    resp = client.put("/file?filename=2&target=4")
    assert resp.status_code == 201

    resp = client.put("/file?filename=1&target=13/37")
    assert resp.status_code == 400
    client.delete("/file?filename=1")
    client.delete("/file?filename=2")
    client.delete("/file?filename=4")
