import pytest
import os

from shutil import rmtree
from storage.dataNode import app as data_node, FILE_STORE


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

    fpath = os.path.join(FILE_STORE, "a.txt")
    f = open(fpath, 'w')
    data = "AAAAAAAAAABBBBBBBBBBBCCCCCCCCCCCCCCCCCCC"
    f.write(data)
    f.close()

    resp = client.get("/file?filename=a.txt")
    assert resp.status_code == 200
    assert resp.data.decode("utf-8") == data

    resp = client.get("/file?filename=b.txt")
    assert resp.status_code == 404

    os.remove(fpath)


def test_file_put(client):
    init_storage_test()

    assert not os.path.exists(os.path.join(FILE_STORE, "test.txt"))
    data = "AAAAAAAAAAABBBBBBBBBBCCCCCCCCCCCCCC"

    resp = client.put("/file?filename=a.txt", data=data)
    assert resp.status_code == 201

    resp = client.put("/file?filename=../b.txt", data=data)
    assert resp.status_code == 400

    resp = client.put("/file?filename=a.txt", data=data)
    assert resp.status_code == 400

    resp = client.put("/file?filename=/folder/b.txt", data=data)
    assert resp.status_code == 404


def test_file_delete(client):
    init_storage_test()
    fpath = os.path.join(FILE_STORE, "a.txt")
    open(fpath, "w")

    assert client.get("/file?filename=a.txt").status_code == 200

    assert client.delete("/file?filename=a.txt").status_code == 200
    assert not os.path.exists(fpath)


def test_mkdir(client):
    init_storage_test()

    resp = client.put("/directory?name=a")

    assert resp.status_code == 201
    assert os.path.exists(os.path.join(FILE_STORE, "a"))

    resp = client.put("/directory?name=a/b/c")
    assert resp.status_code == 400


def test_delete_dir(client):
    init_storage_test()
    dirname = os.path.join(FILE_STORE, "a")
    os.mkdir(dirname)

    resp = client.delete("/directory?name=a")
    assert resp.status_code == 200
    assert not os.path.exists(dirname)

def test_filesystem(client):
    pass