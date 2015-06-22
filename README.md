# R.G.P. (Redis Graph via Python)

RGP provides a simple directed graph database built on top of Redis and utilizes a set of Python classes as its interface. Both vertices and edges can have data and can be queried when traversing the graph.

*Note:* Still in early beta -- the interface may change, there are no tests written, and no performance analysis has been done. This was my way of learning Redis, one of you may find it useful (I think that I will).

## Installation

    pip install -e rgp

## Requrements

* Python 2.7 < 3 (3 support should be simple to do)
* Redis

## Usage

RGP is simple, it is made up of a few core components:

* Nodes -- things that store data -- Vertices and Edges
* Collections -- groupings of nodes
* Traversals -- objects used query the graph. This is heavily insipred by [Tinkerpop's Gremlin](https://github.com/tinkerpop/gremlin/wiki)
* Tokens -- logic used to filter the graph during a traversal

### Adding Data

Here is a very simple graph of a father and son

    from rgp import Graph, Vertex, Edge
    import redis

    connection = redis.StrictRedis(host='localhost', port=6379, db=0)
    graph = Graph(connection)

    dad = Vertex({
        'name': 'Mark',
        'age': 'old'
    })
    son = Vertex({
        'name': 'Jr.',
        'age': 'young'
    })
    parent = Edge('parent', dad, son)
    child = Edge('child', son, dad)
    
    graph.save(parent)
    graph.save(child)

What we have now is a simple graph with a `son` and `dad` vertices and `parent` and `child` edges. 

####Graph

The `Graph` object is the main interface into the database.

##### Methods

* `e` -- Used to get either a edge by id or all edges in the graph. Returns a `Collection`
* `v` -- Used to get a specific vertex by id or all vertices in the graph. Returns a `Collection`
* `traverse` -- Used to create a `Traversal` object. This is registed with the `Graph` instance. Returns a `Traversal`
* `query` -- Used to execute a `Traversal` object. When called without an argument, the last registred traversal will be used.
* `save` -- Used to save a `Node`. If the `Node` is an `Edge`, it will save both `Vertex` objects associated with it. If the argument is a `Collection`, it will loop thorugh and save each `Node`. Retuns an id if the argument were a `Node` or `Collection` otherwise.

####Vertex

A `Vertex` is the base unit of data in the graph. It is how data is stored

####Edge

`Edge` objects are what connect `Vertex` objects in graph -- they make the graph possible. 

### Traversing The Graph

RGP makes graph traversals pretty easy. Each action taken (`Token` executed) during a traversal is esentially a filter against a `Collection` instance. 

#### Traversal

All traversals start and end with a `Collection` object. It could be empty, could be fed one `Node`, or it could be a collection from a previous traversal.

A common way of starting a traversal would be directly from the `Graph` instance:

    trav = graph.traversal(son).outE()

The `traversal` method on `Graph` returns a `Traversal` instance, if called this way the instance is stored on the `Graph` instance. `Traversal` provides a fluid interface so that you can easily chain together `Token` objects to query the graph.

`Traversal` objects can be instantiated directly. This allows for sub-traversals or even prepared statement-like behavior.

    my_trav = Traversal()
    my_trav.outE()

When it is time to run the traversal that was created, you simply call the `query` method with or without a `Traversal` object.

    result = graph.query() #this will run the previous traversal from graph.traverse()
    result = graph.query(my_trav)

#### Tokens

Tokens 

* `outE` --
* `inE` --
* `bothE` --
* `outV` --
* `inV` --
* `bothV` --
* `has` --
* `alias` --
* `back` --
* `loop` --
* `map` -- 
* `filter` --
* `collect` --

#### Custom Tokens

One of the stregths of RGP is the ability to extend the library by adding your own tokens. Tokens must follow a few rules:

* Treat the collection member as immutable. We do this to ensure that we can walk through our traversal and rewind state.
* Must have an `_operator` member. This defines how the `Token` is represented in traversal. 
* Must always return a new `Collection` instance
* If antoher traversal must be run within the `Token`, a new `Traversal` instance is created. Simply calling `graph.traverse` will erase the parent traversal.
* Must have a '__call__' method with a signature.


## License

MIT
