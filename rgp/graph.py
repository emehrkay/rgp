import sys


THIS = sys.modules[__name__]
GRAPH_VARIABLE = 'rgp:index'
GRAPH_VERTEX = 'rgp:vertex'
GRAPH_VERTEX_OUT = 'rgp:vertex_out'
GRAPH_VERTEX_IN = 'rgp:vertex_in'
GRAPH_VERTEX_ID = '__id__'
GRAPH_EDGE = 'rgp:edge'
GRAPH_INDEX = 'rgp:index'
GRAPH_ELEMENT_TYPE = '__type__'
GRAPH_EDGE_LABEL = '__label__'
GRAPH_EDGE_OUT = '__out_v__'
GRAPH_EDGE_IN = '__in_v__'
OPERATORS = {}
MEMO = {}


def memo(element):
    """memoizes the element for future creation"""
    if element._id:
        MEMO[element._id] = element


class RGPException(Exception):
    pass


class RGPEdgeException(RGPException):
    pass


class RGPTokenException(RGPException):
    pass


class RGPTokenAliasException(RGPException):
    pass


class _Collectionable(object):
    pass


class Graph(_Collectionable):

    def __init__(self, redis):
        self.redis = redis

        if not self.redis.exists(GRAPH_VARIABLE):
            self.redis.set(GRAPH_VARIABLE, 0)

    def next_id(self):
        return self.redis.incr(GRAPH_VARIABLE)

    def v(self, _id=None):
        self.traverse().v(_id)
        
        return self.query()

    def e(self, _id=None):
        self.traverse().e(_id)
        
        return self.query()

    def traverse(self, element=None):
        self._traversal = Traversal(element)

        return self._traversal

    def query(self, traversal=None):
        if not traversal:
            traversal = self._traversal

        return traversal.start(self)

    def _add_edge(self, node_id, edge_id, direction='in'):
        key = GRAPH_VERTEX_IN if direction == 'in' else GRAPH_VERTEX_OUT
        key = '%s:%s' % (key, node_id)
        return self.redis.sadd(key, edge_id)

    def save(self, element):
        try:
            _id = element.id if element.id else self.next_id()
            node = isinstance(element, Vertex)
            key = GRAPH_VERTEX if node else GRAPH_EDGE
            key = '%s:%s' % (key, _id)
            data = element.redis_data
            data[GRAPH_ELEMENT_TYPE] = 'node' if node else 'edge'

            if not node:
                out_v = element._out_v
                in_v = element._in_v

                if not out_v or not in_v:
                    msg = """both the out and in nodes must be set
                            before saving an edge"""
                    raise RGPEdgeException(msg)

                out_v_id = self.save(out_v)
                in_v_id = self.save(in_v)
                data[GRAPH_EDGE_OUT] = out_v_id
                data[GRAPH_EDGE_IN] = in_v_id

                self._add_edge(out_v_id, _id, 'in')
                self._add_edge(in_v_id, _id, 'out')

            element.id = _id

            self.redis.hmset(key, data)
            memo(element)

            return _id
        except Exception as e:
            raise e

    def delete(self, element):
        try:
            element
        except Exception as e:
            raise e


class Node(object):

    def __init__(self, data=None, indices=None, dirty=True):
        if data is None:
            data = {}

        self._id = data['_id'] if '_id' in data else None
        self._data = data
        self._dirty = dirty

        memo(self)

    def __setitem__(self, name, value):
        self._data[name] = value
        self._dirty = True

        return self

    def __getitem__(self, name):
        return self._data.get(name, None)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, _id):
        self._id = _id
        self.data[GRAPH_VERTEX_ID] = _id

    @property
    def data(self):
        return self._data

    @property
    def redis_data(self):
        return self.data

    @property
    def key(self):
        _type = GRAPH_EDGE if isinstance(self, Edge) else GRAPH_VERTEX

        return '%s:%s' % (_type, self[GRAPH_VERTEX_ID])


class Vertex(Node):

    @property
    def oute_key(self):
        return '%s:%s' % (GRAPH_VERTEX_OUT, self[GRAPH_VERTEX_ID])

    @property
    def ine_key(self):
        return '%s:%s' % (GRAPH_VERTEX_IN, self[GRAPH_VERTEX_ID])


class Edge(Node):

    def __init__(self, label, out_v=None, in_v=None, data=None):
        super(Edge, self).__init__(data=data)

        self._out_v = out_v
        self._in_v = in_v
        self._label = label

    @property
    def redis_data(self):
        data = self.data
        data[GRAPH_EDGE_LABEL] = self._label
        
        return data

    @property
    def inv_key(self):
        return '%s:%s' % (GRAPH_VERTEX, self[GRAPH_EDGE_IN])

    @property
    def outv_key(self):
        return '%s:%s' % (GRAPH_VERTEX, self[GRAPH_EDGE_OUT])


class Collection(object):

    def __init__(self, data=None):
        self._data = data if data else []
        self._elements = {}

    def __len__(self):
        return len(self._data)

    def __call__(self, *args):
        return Traversal(self)

    def __getitem__(self, key):
        element = self._elements.get(key, None)

        if not element:
            try:
                data = self._data[key]
                kwargs = {
                    'data': data
                }
                etype = 'Vertex' if data[GRAPH_ELEMENT_TYPE] == 'node' else 'Edge'

                if etype is not 'Vertex':
                    kwargs['label'] = data[GRAPH_EDGE_LABEL]

                element = getattr(THIS, etype)(**kwargs)
            except Exception as e:
                raise StopIteration()

        return element

    def __setitem__(self, key, value):
        self._elements[key] = value

    def __delitem__(self, key):
        if key in self._elements:
            del self._models[key]

    @property
    def data(self):
        return self._data


class Traversal(object):

    def __init__(self, collection=None):
        if isinstance(collection, Node):
            collection = Collection([collection.data])

        self.collection = collection
        self.top = Token()
        self.bottom = self.top

    def __call__(self, *args, **kwargs):
        self.bottom._args = args
        self.bottom._kwargs = kwargs

        return self

    def __getattr__(self, name):
        token = OPERATORS.get(name, None)
        
        if not token:
            msg = '%s does not sub-class Token' % name
            raise RGPTokenException(msg)

        node = token(self.collection)

        return self.add_node(node)

    def __getitem__(self, val):
        if type(val) is not slice:
            val = slice(val, None, None)

        node = Range(self.collection)

        return self.add_node(node)

    def add_node(self, node):
        self.bottom.next = node
        self.bottom = node

        return self

    def start(self, graph):
        aliases = {}
        token = self.top.next
        collection = self.collection
        prev = token
        tokens = []
        variable = ''

        while token:
            if isinstance(token, Alias):
                token(*token._args, **token._kwargs)
                aliases[token.name] = token
            elif isinstance(token, Back):
                token(*token._args, **token._kwargs)

                if token.name in aliases:
                    collection = aliases[token.name].collection
                else:
                    msg = """There was no no alias registered with %s""" % token.name
                    raise RGPTokenAliasException(msg)

            token.collection = collection
            token.graph = graph
            collection = token(*token._args, **token._kwargs)
            next = token.next
            prev = token
            token = token.next

        return collection

class _MetaToken(type):
    def __new__(cls, name, bases, attrs):
        cls = super(_MetaToken, cls).__new__(cls, name, bases, attrs)
        _operator = attrs.pop('_operator', None)

        if not _operator:
            msg = '%s token does not nave an un _operator defined' % name
            raise RGPTokenException(msg)

        OPERATORS[_operator] = cls

        return cls


class Token(object):
    __metaclass__ = _MetaToken
    _operator = '__\root\token\__'

    def __init__(self, value=None):
        self.value = value
        self.collection = None
        self.next = None
        self._args = ()
        self._kwargs = {}
        self._range = None

    def __call__(self, *args):
        error = '%s does is not callable' % self.__name__
        raise NotImplementedError(error)

    def compare(self, field, value, comparsion='=='):
        if comparsion == '==':
            return field == value
        elif comparsion == '!=':
            return field != value
        elif comparsion == 'in':
            return field in value


class GetVertex(Token):
    _operator = 'v'

    def __call__(self, _id=None):
        if _id:
            key = '%s:%s' % (GRAPH_VERTEX, _id)
            data = [self.graph.redis.hgetall(key)]
        else:
            key = '%s:*' % GRAPH_VERTEX
            keys = self.graph.redis.keys(key)
            data = []

            for k in keys:
                data.append(self.graph.redis.hgetall(k))

        return Collection(data)


class GetEdge(Token):
    _operator = 'e'

    def __call__(self, _id=None):
        if _id:
            key = '%s:%s' % (GRAPH_EDGE, _id)
            data = [self.graph.redis.hgetall(key)]
        else:
            key = '%s:*' % GRAPH_EDGE
            keys = self.graph.redis.keys(key)
            data = []

            for k in keys:
                data.append(self.graph.redis.hgetall(k))

        return Collection(data)


class Has(Token):
    _operator = 'has'

    def __call__(self, field, value, comparsion='=='):
        data = []

        if field and value:
            for i, node in enumerate(self.collection.data):
                if field in node and self.compare(node[field], value, comparsion):
                    data.append(node)

        return Collection(data)


class Contains(Token):
    _operator = 'contains'


class Alias(Token):
    _operator = 'alias'

    def __call__(self, name):
        self.name = name

        return self.collection


class Back(Token):
    _operator = 'back'

    def __call__(self, name):
        self.name = name

        return self.collection


class Range(Token):
    _operator = '_doesnt/really/matter_'


class Filter(Token):
    _operator = 'filter'

    def __call__(self, callback):
        data = filter(callback, self.collection.data)

        return Collecton(data)


class Map(Token):
    _operator = 'map'

    def __call__(self, callback):
        data = map(callback, self.collection.data)

        return Collecton(data)


class OutE(Token):
    _operator = 'outE'

    def __call__(self):
        data = []

        for i, node in enumerate(self.collection):
            if isinstance(node, Edge):
                self.graph.traverse(node).outV()
                data.extend(self.graph.query().data)
            else:
                edges = self.graph.redis.smembers(node.oute_key)
            
                for d in edges:
                    data.extend(list(self.graph.e(d).data))

        return Collection(data)


class InE(Token):
    _operator = 'inE'

    def __call__(self):
        data = []

        for i, node in enumerate(self.collection):
            if isinstance(node, Edge):
                self.graph.traverse(node).inV()
                data.extend(self.graph.query().data)
            else:
                edges = self.graph.redis.smembers(node.ine_key)
            
                for d in edges:
                    data.extend(list(self.graph.e(d).data))

        return Collection(data)


class BothE(Token):
    _operator = 'bothE'

    def __call__(self):
        data = []
        
        def get_edge(key):
            data = []
            edges = self.graph.redis.smembers(key)

            for d in edges:
                data.extend(list(self.graph.e(d).data))

            return data

        for i, node in enumerate(self.collection):
            if isinstance(node, Edge):
                self.graph.traverse(node).inV()
                data.extend(self.graph.query().data)
                self.graph.traverse(node).outV()
                data.extend(self.graph.query().data)
            else:
                data.extend(get_edge(node.ine_key))
                data.extend(get_edge(node.oute_key))

        return Collection(data)


class OutV(Token):
    _operator = 'outV'

    def __call__(self):
        data = []

        for i, node in enumerate(self.collection):
            if isinstance(node, Vertex):
                trav = Traversal(node)
                trav.outE()
                data = [self.graph.redis.hgetall(n.outv_key) for n in self.graph.query(trav)]
            else:
                inv = self.graph.redis.hgetall(node.outv_key)

                data.append(inv)

        return Collection(data)


class InV(Token):
    _operator = 'inV'

    def __call__(self):
        data = []

        for i, node in enumerate(self.collection):
            if isinstance(node, Vertex):
                trav = Traversal(node)
                trav.inE()
                data = [self.graph.redis.hgetall(n.outv_key) for n in self.graph.query(trav)]
            else:
                inv = self.graph.redis.hgetall(node.inv_key)

                data.append(inv)

        return Collection(data)


class BothV(Token):
    _operator = 'bothV'

    def __call__(self):
        data = []

        for i, node in enumerate(self.collection):
            if isinstance(node, Edge):
                trav = Traversal(node)
                trav.outE()
                out_v = [self.graph.redis.hgetall(n.outv_key) for n in self.graph.query(trav)]
                trav = Traversal(node)
                trav.inE()
                in_v = [self.graph.redis.hgetall(n.outv_key) for n in self.graph.query(trav)]
                data.extend(out_v)
                data.extend(in_v)
            else:
                data.extend(self.graph.redis.hgetall(node.inv_key))
                data.extend(self.graph.redis.hgetall(node.outv_key))

        return Collection(data)

    
