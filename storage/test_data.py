import pytest
import os

from shutil import rmtree
from storage.data_node import app as data_node, FILE_STORE


@pytest.fixture
def client():
    data_node.config['TESTING'] = True

    with data_node.test_client() as client:
        yield client


def init_storage_test():
    rmtree(FILE_STORE)
    os.mkdir(FILE_STORE)


def test_file_get(client):
    init_storage_test()

    fpath = os.path.join(FILE_STORE, "1")
    f = open(fpath, 'w')
    data = "AAAAAAAAAABBBBBBBBBBBCCCCCCCCCCCCCCCCCCC"
    f.write(data)
    f.close()

    resp = client.get("/file?filename=1")
    assert resp.status_code == 200
    assert resp.data.decode("utf-8") == data

    resp = client.get("/file?filename=2")
    assert resp.status_code == 404

    os.remove(fpath)


def test_file_put(client):
    init_storage_test()

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
