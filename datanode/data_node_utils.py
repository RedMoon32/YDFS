class DataNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = int(port)

    def __eq__(self, other):
        return self.ip == other.ip and self.port == other.port

    def serialize(self):
        return {"ip": self.ip, "port": self.port}
