import redis
from graph import *
import time


start_time = time.time()
r = redis.StrictRedis(host='localhost', port=6379, db=0)

g = Graph(r)
id = ['name', 'sex']

# r.flushdb()
# for i in range(10000):
#     n = Vertex({
#         'name': 'mark',
#         'role': 'dad',
#         'sex': 'male',
#         '__indices': id
#     })
#     n2 = Vertex({
#         'name': 'jr',
#         'role': 'child',
#         'sex': 'male',
#         '__indices': id
#     })
#     n3 = Vertex({
#         'name': 'leanne',
#         'role': 'mom',
#         'sex': 'female',
#         '__indices': id
#     })
#     s = Vertex({
#         'name': 'sade',
#         'sex': 'female',
#         '__indices': id
#     })
#     sam = Vertex({
#         'name': 'sam',
#         'sex': 'female',
#         '__indices': id
#     })
#     e = Edge('father', n, n2)
#     e2 = Edge('fson', n2, n)
#     e3 = Edge('mother', n3, n2)
#     e4 = Edge('mson', n2, n3)
#     e5 = Edge('so', n, s)
#     e6 = Edge('so', s, n)
#     e7 = Edge('sister', s, sam)
#     e8 = Edge('sister', sam, s)
#     col = Collection()
#     col.append(e).append(e2).append(e3).append(e4).append(e5).append(e6).append(n).append(n2).append(n3).append(s)
#     col.append(e7).append(e8)
#     g.save(col)

print '+++++++++++++++++++++++++++++++'
print '%s seconds to finish adding %s nodes' % (time.time() - start_time, len(r.keys()))

# g.save(e)
# g.save(e2)
# g.save(e3)
# g.save(e4)
# g.save(e5)
# g.save(e6)
# print r.keys()
# print g.v(9).data
#
# print r.smembers('rgp:vertex_out:3')
# tr = g.traverse(n2)
# tr.outE().outV().has('name', ['mark', 'leanne'], 'in')
#print r.keys(), tr.outE(0, name='mark'), tr.bottom
# col = g.query(tr)
#g.traverse(n2).alias('t').outV().alias('x').has('name', 'mark').outE().outV().back('x').back('t')
# print g.e().data
# print g.v().data
# g.traverse(n2).outV().outV().outV().has('name', 'sam', '==')
# print 'first'
# for i in g.query():
#     print 'result::', i, i.data
#
#
# g.traverse(n2).alias('s').outV().loop('s', 2).has('name', 'sam')
# print 'with loop'
# for i in g.query():
#     print 'result::', i, i.data
#
# g.traverse(n2).alias('x').outV().loop('x', 2).has('name', 'sam').alias('y').collect('x', 'y')
# print 'with collect'
# for i in g.query():
#     print 'result::', i, i.data
#
#
# print '==============='
# print n2.id
# print len(r.keys()), r.keys()
#
# g.delete(n2)
#
# print '==============='
# print len(r.keys()), r.keys()

st = time.time()

g.traverse().get('sex', 'male').get('name', 'jr')[40:80].alias('x').outV().loop('x', 2).has('name', 'sam')
res = g.query()
# for i in g.query():
#     print i, i.data

print '+++++++++++++++++++++++++++++++'
print '%s seconds to query %s results' % (time.time() - st, len(res))

for it in res:
    print it, it.data
