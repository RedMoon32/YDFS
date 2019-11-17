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
        if filename in self._file_mapper or not self.dir_exists(os.path.dirname(filename)):
            return None
        else:
            self._id += 1
            self._file_mapper[filename] = \
                File(filename, self._id, [], {'created_at': time.time()})
            return self._file_mapper[filename]

    def get_file(self, filename) -> File:
        return self._file_mapper.get(filename, None)

    def add_directory(self, dirname) -> bool:
        if not self.dir_exists(dirname) and self.dir_exists(os.path.join(dirname, '..')):
            self._dirs.append(dirname)
            return True
        return False

    def dir_exists(self, dirname):
        dirname = "/" if dirname == "" else dirname
        return dirname in self._dirs

    def rename_file(self, file_name, new_file_name):
        if file_name in self._file_mapper:
            self._file_mapper[new_file_name] = self._file_mapper.pop(file_name)
            self._file_mapper[new_file_name].name = new_file_name
        else:
            raise FileNotFoundError
