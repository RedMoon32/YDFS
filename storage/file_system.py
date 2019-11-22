import time
import os


class File:

    def __init__(self, file_name, id, nodes, file_info):
        self.name = file_name
        self.id = id
        self.nodes = nodes
        self.file_info = file_info

    def serialize(self):
        return {'file_name': self.name,
                'file_id': self.id,
                'nodes': list(i.serialize() for i in self.nodes),
                'file_info': self.file_info}


class FileSystem:

    def __init__(self):
        # dict {file_path<a.txt>: {file_id: 0, nodes: [DataNode()], file_info {created_at:000} }
        self._file_mapper = {}
        # array of [{dir_path</a/b>:'vv',dir_info:  {created_at:000}}]
        self._dirs = ['/']
        self._id = 0

    def add_file(self, filename) -> File:
        if filename in self._file_mapper:
            raise Exception(f"File '{filename}' already exists")
        if not self.dir_exists(os.path.dirname(filename)):
            raise Exception(f"Directory '{os.path.dirname(filename)}' not found")
        else:
            self._id += 1
            self._file_mapper[filename] = \
                File(filename, self._id, [], {'created_at': time.time()})
            return self._file_mapper[filename]

    def get_file(self, filename) -> File:
        return self._file_mapper.get(filename, None)

    def file_exists(self, filename):
        return filename in self._file_mapper

    def add_directory(self, dirname):
        if not self.dir_exists(dirname) and self.dir_exists(os.path.dirname(dirname)):
            self._dirs.append(dirname)
        else:
            raise FileExistsError

    def dir_exists(self, dirname):
        dirname = "/" if dirname == "" else dirname
        return dirname in self._dirs

    def get_subdirs(self, dirname):
        return [dir for dir in self._dirs if dir != dirname and self.file_in_directory(dir, dirname)]

    def file_in_directory(self, filename, dirname):
        return os.path.dirname(filename) == dirname

    def get_files(self, dirname):
        # O(n) getting list of files in dir
        return [self._file_mapper[file] for file in self._file_mapper if self.file_in_directory(file, dirname)]

    def rename_file(self, file_name, new_file_name):
        if file_name in self._file_mapper:
            self._file_mapper[new_file_name] = self._file_mapper.pop(file_name)
            self._file_mapper[new_file_name].name = new_file_name
        else:
            raise FileNotFoundError
