import pytest

from storage.masterNode import app as master_node, DataNode
import storage.masterNode


@pytest.fixture
def client():
    master_node.config['TESTING'] = True

    with master_node.test_client() as client:
        yield client


def clean():
    storage.masterNode.file_mapper = {}
    storage.masterNode.data_nodes = []


def test_register_datanode(client):
    resp = client.post("/datanode?ip=101.101.101.101&port=2000")
    assert resp.status_code == 201

    resp = client.post("/datanode?ip=101.101.101.101&port=2000")
    assert resp.status_code == 400

    clean()


def test_file_location_to_store(client):
    resp = client.post("/datanode?ip=101.101.101.101&port=2000")
    assert resp.status_code == 201

    resp = client.post("/file")
    assert resp.json == {"ip": "101.101.101.101",
                         "port": 2000}

    clean()


def test_file_node_locations(client):
    storage.masterNode.file_mapper = {"file1": [DataNode("127.0.0.1", "333"),
                                                DataNode("127.0.0.2", "333")]}
    resp = client.get("/file?filename=file1")
    assert resp.json == [{"ip": "127.0.0.1",
                          "port": 333},
                         {"ip": "127.0.0.2",
                          "port": 333}]

    clean()
