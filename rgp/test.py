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
e = Edge('father', n, n2)
e2 = Edge('son', n2, n)
e3 = Edge('mother', n3, n2)
e4 = Edge('son', n2, n3)
g.save(e)
g.save(e2)
g.save(e3)
g.save(e4)

# tr = g.traverse(n2)
# tr.outE().outV().has('name', ['mark', 'leanne'], 'in')
#print r.keys(), tr.outE(0, name='mark'), tr.bottom
# col = g.query(tr)
g.traverse(n2).outV()
for i in g.query():
    print 'result::', i, i.data

    