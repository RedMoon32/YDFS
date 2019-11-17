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
        self._dirs = []
        self._id = 0

    def add_file(self, filename) -> File:
        if filename in self._file_mapper:
            return None
        else:
            self._id += 1
            self._file_mapper[filename] = \
                File(filename, self._id, [], {'created_at': time.time()})
            return self._file_mapper[filename]

    def get_file(self, filename) -> File:
        return self._file_mapper.get(filename, None)

    def dir_can_be_created(self, dirname):
        rel_paths = [os.path.relpath(dirname, fdir) for fdir in self._dirs]
        return '..' in rel_paths or '.' in rel_paths

    def add_directory(self, dirname) -> bool:
        if dirname not in self.dir_can_be_created(dirname):  # and upper directory exists
            self._dirs.append(dirname)
            return True
        return False

    def dir_exists(self, dirname):
        return dirname in self._dirs
