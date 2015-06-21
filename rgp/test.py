import redis
from graph import *
r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.flushdb()
g = Graph(r)
n = Vertex({
    'name': 'mark',
    'role': 'dad'
})
n2 = Vertex({
    'name': 'jr',
    'role': 'child'
})
n3 = Vertex({
    'name': 'leanne',
    'role': 'mom'
})
s = Vertex({
    'name': 'sade',
})
e = Edge('father', n, n2)
e2 = Edge('fson', n2, n)
e3 = Edge('mother', n3, n2)
e4 = Edge('mson', n2, n3)
e5 = Edge('so', n, s)
e6 = Edge('so', s, n)
g.save(e)
g.save(e2)
g.save(e3)
g.save(e4)
g.save(e5)
g.save(e6)

# tr = g.traverse(n2)
# tr.outE().outV().has('name', ['mark', 'leanne'], 'in')
#print r.keys(), tr.outE(0, name='mark'), tr.bottom
# col = g.query(tr)
g.traverse(n2).alias('t').outV().alias('x').has('name', 'mark').outE().outV().back('x').back('t')
for i in g.query():
    print 'result::', i, i.data

    