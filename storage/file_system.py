import time
import os


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
        return self.name == other.name \
               and self.id == other.id and self.file_info == other.file_info


class FileSystem:
    def __init__(self):
        # dict {file_path<a.txt>: {file_id: 0, nodes: [DataNode()], file_info {created_at:000} }
        self._file_mapper = {}
        # array of [{dir_path</a/b>:'vv',dir_info:  {created_at:000}}]
        self._dirs = ["/"]
        self._id = 0

    def add_file(self, filename) -> File:
        if filename in self._file_mapper:
            raise Exception(f"File '{filename}' already exists")
        if not self.dir_exists(os.path.dirname(filename)):
            raise Exception(f"Directory '{os.path.dirname(filename)}' not found")
        else:
            self._id += 1
            self._file_mapper[filename] = File(
                filename, self._id, [], {"created_at": time.time()}
            )
            return self._file_mapper[filename]

    def get_file(self, filename) -> File:
        return self._file_mapper.get(filename, None)

    def file_exists(self, filename):
        return filename in self._file_mapper

    def add_directory(self, dirname):

        if dirname == "":
            raise ValueError("Empty name not allowed")
        if self.dir_exists(dirname):
            raise FileExistsError(f"Directory '{dirname}' already exists")
        if self.get_file(dirname) is not None:
            raise ValueError(f"Already exists the file named '{dirname}'")
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
        for file in self._file_mapper:
            if self._file_mapper[file].id == file_id:
                return self._file_mapper[file]
        return None

    def get_files(self, dirname):
        # O(n) getting list of files in dir
        return [
            self._file_mapper[file]
            for file in self._file_mapper
            if self.file_in_directory(file, dirname)
        ]

    def move_file(self, file_name, destination):
        new_file_name = os.path.join(destination, os.path.basename(file_name))

        if not self.dir_exists(destination):
            raise FileNotFoundError("No directory found")

        if self.get_file(new_file_name):
            raise FileExistsError("File already exists")

        if file_name in self._file_mapper:
            self._file_mapper[new_file_name] = self._file_mapper.pop(file_name)
            self._file_mapper[new_file_name].name = new_file_name
        else:
            raise FileNotFoundError("No file found")

    def remove_file(self, file_name):
        if not file_name in self._file_mapper:
            raise FileNotFoundError("File not found")
        else:
            self._file_mapper.pop(file_name)

    def remove_dir(self, dirname):
        if dirname not in self._dirs:
            raise FileNotFoundError("Directory not found")
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
