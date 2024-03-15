class Tracker:
    def __init__(self, vertex_id, edge_id):
        self._vertex_id = vertex_id + 1
        self._edge_id = edge_id + 1
        self._main_vertex_id = vertex_id + 1

    @property
    def vertex_id(self):
        return self._vertex_id

    @vertex_id.setter
    def vertex_id(self, value):
        self._vertex_id += 1

    @property
    def edge_id(self):
        return self._edge_id

    @edge_id.setter
    def edge_id(self, value):
        self._edge_id += 1

    @property
    def main_vertex_id(self):
        return self._main_vertex_id
