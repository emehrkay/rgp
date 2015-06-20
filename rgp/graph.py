import sys


THIS = sys.modules[__name__]
GRAPH_VARIABLE = 'rgp:index'
GRAPH_NODE = 'rgp:node'
GRAPH_NODE_OUT = 'rgp:node_out'
GRAPH_NODE_IN = 'rgp:node_in'
GRAPH_NODE_ID = '__id__'
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


class _Collectionable(object):
    pass


class Graph(_Collectionable):

    def __init__(self, redis):
        self.redis = redis

        if not self.redis.exists(GRAPH_VARIABLE):
            self.redis.set(GRAPH_VARIABLE, 0)

    def next_id(self):
        return self.redis.incr(GRAPH_VARIABLE)

    def v(self, id=None):
        key = '%s:%s' % (GRAPH_NODE, id)
        res = self.redis.hgetall(key)
        col = Collection(self, [res])
        
        return col

    def e(self, id=None):
        key = '%s:%s' % (GRAPH_EDGE, id)
        res = self.redis.hgetall(key)
        col = Collection(self, [res])
        
        return col

    def traverse(self, element):
        arg = element if isinstance(element, Collection) else Collection(self, [element._data])
        self._traversal = Traversal(arg)

        return self._traversal

    def query(self, traversal):
        return traversal.eval(self)

    def _add_edge(self, node_id, edge_id, direction='in'):
        key = GRAPH_NODE_IN if direction == 'in' else GRAPH_NODE_OUT
        key = '%s:%s' % (key, node_id)
        return self.redis.sadd(key, edge_id)

    def save(self, element):
        try:
            _id = element.id if element.id else self.next_id()
            node = isinstance(element, Node)
            key = GRAPH_NODE if node else GRAPH_EDGE
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

                self._add_edge(out_v_id, _id, 'out')
                self._add_edge(in_v_id, _id, 'in')

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


class Element(object):

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
        self.data[GRAPH_NODE_ID] = _id

    @property
    def data(self):
        return self._data

    @property
    def redis_data(self):
        return self.data


class Node(Element):
    pass


class Edge(Element):

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

class Collection(object):

    def __init__(self, graph, data=None):
        self._data = data if data else []
        self._elements = {}

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
                etype = 'Node' if data[GRAPH_ELEMENT_TYPE] == 'node' else 'Edge'

                if etype is not 'Node':
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
        self.collection = collection
        self.top = Token()
        self.bottom = self.top

    def __call__(self, *args, **kwargs):
        print 'calling', args, kwargs
        
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

    def eval(self, graph):
        aliases = {}
        token = self.top.next
        collection = self.collection
        prev = token
        tokens = []
        variable = ''

        while token:
            collection = token(collection, graph)
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

    def __call__(self, *args):
        error = '%s does is not callable' % self.__name__
        raise NotImplementedError(error)


class Has(Token):
    _operator = 'has'


class Contains(Token):
    _operator = 'contains'


class Alias(Token):
    _operator = 'alias'


class Back(Token):
    _operator = 'back'


class Range(Token):
    _operator = '_doesnt/really/matter_'


class Filter(Token):
    _operator = 'filter'


class OutE(Token):
    _operator = 'outE'

    def __call__(self, collection, graph):
        data = []

        for i, node in enumerate(collection):
            key = '%s:%s' % (GRAPH_NODE_OUT, node[GRAPH_NODE_ID])
            edges = graph.redis.smembers(key)
            
            for d in edges:
                data.extend(list(graph.e(d).data))

        return Collection(graph, data)


class InE(Token):
    _operator = 'inE'


class OutV(Token):
    _operator = 'outV'


class InV(Token):
    _operator = 'inV'


