#!/usr/bin/env python

import random, sys, os, requests, json, uuid, redis, subprocess, time, shutil, math, string

base = os.path.dirname(os.path.abspath(os.path.join(__file__, '..')))
key = sys.argv[1] if len(sys.argv) > 1 else ''.join(random.sample((string.ascii_uppercase + string.digits)*10, 10))

# Seed the random number generator with a know value
random.seed(key)

n = random.randrange(3,6)
limit = 5*1024*1024
port = 5555

log = os.path.join(base, 'var', 'log')
db = os.path.join(base, 'var', 'db')

if os.path.exists(log): shutil.rmtree(log)
if os.path.exists(db): shutil.rmtree(db)

os.makedirs(log)
os.makedirs(db)

configs = [ { 'id': str(i), 'host': 'localhost', 'port': port+i } for i in range(n) ]
processes =	[ subprocess.Popen(['redis-server', '--port', str(config['port']), '--bind', '127.0.0.1', '--logfile', os.path.join(log, 'server'+config['id']+'.log'), '--dbfilename', 'server'+config['id']+'.rdb', '--databases', '1', '--maxmemory', str(limit), '--maxmemory-policy', 'noeviction', '--dir', db ]) for config in configs ]
clients = [ redis.StrictRedis(host=config['host'], port=config['port'], db=0) for config in configs ]

server = subprocess.Popen(['python', os.path.join(base, 'server.py'), json.dumps({ 'servers': configs })])


# Import the list of things to rate
entities = open(os.path.join(base, 'test', 'entities.txt')).read().splitlines()

endpoint = 'http://localhost:2500'


def get(id):
	headers = { 'Accept': 'application/json' }
	data = requests.get(endpoint+'/rating/'+id, headers=headers).json()
	return data['rating']

def put(id, source, rating):
	headers = { 'Accept': 'application/json', 'Content-type': 'application/json' }
	data = json.dumps({ 'rating': rating, 'source': source })
	requests.put(endpoint+'/rating/'+id, headers=headers, data=data)

def result(r):
	print(json.dumps(r))

def flush():
	for client in clients:
		client.flushdb()

def count():
	return sum(map(lambda c:c.info()['total_commands_processed'],clients))

def sum(l):
	return reduce(lambda s,a: s+a, l, float(0))

def mean(l):
	return sum(l)/len(l)

def variance(l):
	m = mean(l)
	return map(lambda x: (x - m)**2, l)

def stddev(l):
	return math.sqrt(mean(variance(l)))

def usage():
	return map(lambda c:c.info()['used_memory'],clients)

def mksources(n = 1):
	return [ str(uuid.uuid1()) for i in range(n) ]

print("Running test #"+key)

# Give the server some time to start up
time.sleep(3)

c = count()

flush()
# Initial check to ensure null ratings come back for entities
for e in random.sample(entities, 5):
	result({ 'type': 'EXPECT_RATING', 'entity': e, 'rating': get(e), 'expected': None })

# Check to see if the student is actually using redis by monitoring how
# many commands have been run between now and then
result({ 'type': 'COMMANDS_RUN', 'count': count() - c })

# Put in some ratings for things
s = mksources()[0]
for e in random.sample(entities, 5):
	value = random.randrange(1,10)
	put(e, s, value)
	result({ 'type': 'EXPECT_RATING', 'entity': e, 'rating': get(s), 'expected': value })

# Update some ratings
for e in random.sample(entities, 5):
	value = random.randrange(1,10)
	put(e, s, value)
	result({ 'type': 'EXPECT_RATING', 'entity': e, 'rating': get(s), 'expected': value })

flush()
# Add ratings from different sources
for e in random.sample(entities, 30):
	ratings = [ { 'source': s, 'rating': random.randrange(1,10) } for s in random.sample(mksources(50), 30) ]
	rating = mean(map(lambda a: a['rating'], ratings))
	for r in ratings: put(e, r['source'], r['rating'])
	result({ 'type': 'EXPECT_RATING', 'entity': e, 'rating': get(e), 'expected': rating })

# Pump enough ratings to make use of shards by monitoring the redis memory usage; we keep
# going until 75% of all memory is filled or until we've hit some large number of ratings 
# that have been put in or if the memory usage isn't increasing between runs.
i = 0
last = 0
perf = [ ]
while(last < sum(usage()) < (0.75*n*limit) or i > 100000):
	last = sum(usage())
	for e in random.sample(entities, 10):
		sources = mksources(1000)
		ratings = [ { 'source': s, 'rating': random.randrange(1,10) } for s in random.sample(sources, random.randrange(1, len(sources))) ]
		rating = mean(map(lambda a: a['rating'], ratings))
		start = time.time()
		for r in ratings: put(e, r['source'], r['rating'])
		end = time.time()
		duration = end - start
		perf.append(duration/len(ratings))
		result({ 'type': 'EXPECT_RATING', 'entity': e, 'rating': get(e), 'expected': rating, 'samples': len(ratings), 'duration': duration })
		i = i + len(ratings)

u = usage()
# How much of the data store has been used
result({ 'type': 'EXPECTED_USAGAE', 'minimum': 0.75*n*limit, 'result': sum(u) })
# What is the distribution like between data stores
result({ 'type': 'USAGE_STDDEV', 'result': stddev(u) })
# How many ratings did we manage to throw in
result({ 'type': 'TOTAL_RATINGS', 'result': i })
# How fast was it approximately
result({ 'type': 'PERF_AVG', 'result': mean(perf) })
result({ 'type': 'PERF_STDDEV', 'result': stddev(perf) })

# Shut. down. everything.
server.terminate()
for p in processes: p.terminate()

# Fin.
