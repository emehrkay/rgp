import redis
from graph import *
r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.flushdb()
g = Graph(r)
n = Node({
    'name': 'mark'
})
n2 = Node({
    'name': 'jr'
})
e = Edge('father', n, n2)
e2 = Edge('son', n2, n)
g.save(e)
g.save(e2)

tr = g.traverse(n2)
tr.outE(0, name='mark')
#print r.keys(), tr.outE(0, name='mark'), tr.bottom
col = g.query(tr)

for i in col:
    print i, i.data