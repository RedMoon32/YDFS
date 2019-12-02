import time
import os
import copy


class DataNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = int(port)

    def __eq__(self, other):
        return self.ip == other.ip and self.port == other.port

    def serialize(self):
        return {"ip": self.ip, "port": self.port}


class File:
    def __init__(self, file_name, id, nodes, file_info):
        self.name = file_name
        self.id = id
        self.nodes = nodes
        self.file_info = file_info

    def serialize(self):
        return {
            "file_name": self.name,
            "file_id": self.id,
            "nodes": list(i.serialize() for i in self.nodes),
            "file_info": self.file_info,
        }

    def __eq__(self, other):
        return (
                self.name == other.name
                and self.id == other.id
                and self.file_info == other.file_info
        )

    def __hash__(self):
        return self.id


class FileSystem:
    def __init__(self):
        # dict {file_path<a.txt>: {file_id: 0, nodes: [DataNode()], file_info {created_at:000} }
        self._file_mapper = {}
        # stores same files by references as file_mapper but instead of name it uses file_id
        self._file_id_mapper = {}
        # array of [{dir_path</a/b>:'vv',dir_info:  {created_at:000}}]
        self._dirs = ["/"]
        self._id = 0

    def add_file(self, filename) -> File:
        if filename in self._file_mapper:
            raise FileExistsError(f"file '{filename}' already exists")
        if not self.dir_exists(os.path.dirname(filename)):
            raise FileNotFoundError(f"directory '{os.path.dirname(filename)}' not found")
        if self.dir_exists(filename):
            raise FileExistsError(f"already exists the directory named '{filename}', forgot to specify a filename?")
        else:
            self._id += 1
            new_file = File(filename, self._id, [], {
                "created_at": time.time(),
                "last_accessed": time.time(),
                "size": 0})
            self._file_mapper[filename] = new_file
            self._file_id_mapper[new_file.id] = new_file
            return new_file

    def get_file(self, filename) -> File:
        return self._file_mapper.get(filename, None)

    def file_exists(self, filename):
        return filename in self._file_mapper

    def add_directory(self, dirname):

        if dirname == "":
            raise ValueError("empty name not allowed")
        if self.dir_exists(dirname):
            raise FileExistsError(f"directory '{dirname}' already exists")
        if self.get_file(dirname) is not None:
            raise ValueError(f"already exists the file named '{dirname}'")
        if not self.dir_exists(os.path.dirname(dirname)):
            raise ValueError(
                f"Upper directory '{os.path.dirname(dirname)}' does not exist"
            )

        self._dirs.append(dirname)

    def dir_exists(self, dirname):
        dirname = "/" if dirname == "" else dirname
        return dirname in self._dirs

    def get_subdirs(self, dirname):
        return [
            dir
            for dir in self._dirs
            if dir != dirname and os.path.dirname(dir) == dirname
        ]

    def file_in_directory(self, filename, dirname):
        return os.path.dirname(filename) == dirname

    def get_file_by_id(self, file_id):
        return self._file_id_mapper.get(file_id, None)

    def get_all_files(self):
        return list(self._file_mapper.values())

    def get_files(self, dirname):
        # O(n) getting list of files in dir
        return [
            self._file_mapper[file]
            for file in self._file_mapper
            if self.file_in_directory(file, dirname)
        ]

    def get_all_ids(self):
        return list(self._file_id_mapper.keys())

    def move_file(self, file_name, destination):
        new_file_name = os.path.join(destination, os.path.basename(file_name))

        if not self.dir_exists(destination):
            raise FileNotFoundError(f"directory '{destination}' not found")

        if self.get_file(new_file_name):
            raise FileExistsError(f"file '{new_file_name}' already exists")

        if file_name in self._file_mapper:
            self._file_mapper[new_file_name] = self._file_mapper.pop(file_name)
            self._file_mapper[new_file_name].name = new_file_name
        else:
            raise FileNotFoundError(f"file '{file_name}' found")

    def copy_file(self, file_name, new_file_name):  # ToDO: remove copying
        destination = os.path.dirname(new_file_name)

        if not self.dir_exists(destination):
            raise FileNotFoundError(f"directory '{destination}' not found")

        if self.dir_exists(new_file_name):
            raise FileExistsError(f"already exists the directory named '{new_file_name}', forgot to specify a filename?")

        if self.get_file(new_file_name):
            raise FileExistsError(f"file '{new_file_name}' already exists")

        if file_name in self._file_mapper:
            self._id += 1
            self._file_mapper[new_file_name] = copy.deepcopy(self.get_file(file_name))
            self._file_mapper[new_file_name].id = self._id
            self._file_mapper[new_file_name].file_info["created_at"] = time.time()
            self._file_mapper[new_file_name].name = new_file_name
            self._file_id_mapper[self._id] = self._file_mapper[new_file_name]
        else:
            raise FileNotFoundError(f"file '{file_name}' found")

    def remove_file(self, file_name):
        file = self.get_file(file_name)
        if file is None:
            raise FileNotFoundError(f"file '{file_name}' not found")
        else:
            self._file_mapper.pop(file.name)
            self._file_id_mapper.pop(file.id)

    def remove_dir(self, dirname):
        if dirname not in self._dirs:
            raise FileNotFoundError(f"directory '{dirname}' not found")
        self._dirs.remove(dirname)
        dirname = dirname + "/" if dirname[-1] != "/" else dirname
        files = []
        for file in self._file_mapper.values():
            if file.name.startswith(dirname):
                # list of files which must be deleted from datanodes
                files.append(file)
        for dir in self._dirs[:]:
            if dir.startswith(dirname):
                self._dirs.remove(dir)
        return files
