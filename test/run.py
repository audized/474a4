#!/usr/bin/env python

import random, sys, os, requests, json, uuid, redis, subprocess, time, shutil, math, string, itertools, argparse

parser = argparse.ArgumentParser(description='Process.')

parser.add_argument('--key', dest='key', action='store', nargs='?', default=''.join(random.sample((string.ascii_uppercase + string.digits)*10, 10)), help='random nonce')
parser.add_argument('--results', dest='output', action='store', nargs='?', type=argparse.FileType('w'), default=sys.stdout, help='file to store results to (default stdout)')
args = parser.parse_args()

base = os.path.dirname(os.path.abspath(os.path.join(__file__, '..')))

# Seed the random number generator with a known value
random.seed(args.key)

n = random.randrange(3,6)
limit = 2*1024*1024
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
	args.output.write(json.dumps(r)+'\n')

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

print("Running test #"+args.key)

# Some general information
result({ 'name': 'info', 'type': 'SHARD_COUNT', 'value': n })
result({ 'name': 'info', 'type': 'SHARD_SIZE', 'value': limit })

# Give the server some time to start up
time.sleep(1)

tests = [ ]
def test():
	def wrapper(f):
		def rx(obj):
			x = obj.copy()
			obj['name'] = f.__name__
			result(obj)
		def wrapped(*a):
			print("Running test %s" % (f.__name__))
			# Clean the database before subsequent tests
			flush()
			# Reset the RNG to a known value
			random.seed(args.key+'/'+f.__name__)
			f(rx, *a)
		tests.append(wrapped)
		return wrapped
	return wrapper

@test()
def emptiness(result):
	# Initial check to ensure null ratings come back for entities
	for e in random.sample(entities, 5):
		result({ 'type': 'EXPECT_RATING', 'entity': e, 'got': get(e), 'expected': None })

# Check to see if the student is actually using redis by monitoring how
# many commands have been run between now and then
@test()
def commands(result):	
	c = count()
	s = 'test'
	put(entities[0], s, random.randrange(1,10))
	result({ 'type': 'COMMANDS_RUN', 'count': count() - c })

# Put in some ratings for things
@test()
def simple(result):
	s = 'test'
	for e in random.sample(entities, 5):
		value = float(random.randrange(1,10))
		put(e, s, value)
		result({ 'type': 'EXPECT_RATING', 'entity': e, 'got': get(e), 'expected': value })

# Update some ratings
@test()
def updates(result):
	s = 'test'
	for e in random.sample(entities, 5):
		put(e, s, random.randrange(1,10))
	for e in random.sample(entities, 5):
		value = float(random.randrange(1,10))
		put(e, s, value)
		result({ 'type': 'EXPECT_RATING', 'entity': e, 'got': get(e), 'expected': value })

# Add ratings from different sources
@test()
def sources(result):
	for e in random.sample(entities, 30):
		ratings = [ { 'source': s, 'rating': random.randrange(1,10) } for s in random.sample(mksources(50), 30) ]
		rating = mean(map(lambda a: a['rating'], ratings))
		for r in ratings: put(e, r['source'], r['rating'])
		result({ 'type': 'EXPECT_RATING', 'entity': e, 'got': get(e), 'expected': rating })



@test()
def performance(result):

	perf = [ ]
	e = entities[0]

	def pulse(n):
		start = time.time()
		for source in mksources(n):
			put(e, source, random.randrange(1,10))
		for _ in range(n*3):
			get(e)
		end = time.time()
		perf.append((end - start)/n)
	
	pulse(1)
	pulse(10)
	pulse(100)
	pulse(1000)
	# pulse(10000) Too heavy for small sons
		
	# How fast was it approximately
	result({ 'type': 'MEAN', 'result': mean(perf) })
	result({ 'type': 'STDDEV', 'result': stddev(perf) })

# Pump enough ratings to make use of shards by monitoring the redis memory usage; we keep
# going until 75% of all memory is filled or until we've hit some large number of ratings 
# that have been put in or if the memory usage isn't increasing between runs or if one
# server has almost been completely filled up.
@test()
def sharding(result):
	i = 0
	last = 0
	
	ratings = [ ]
	us = usage()

	while(last < sum(usage()) < (0.75*n*limit) or i < 10 or any(float(u)/float(limit) > 0.95 for u in usage())):
		last = sum(usage())
		for e in random.sample(entities, 10):
			sources = mksources(200)
			local = [ { 'target': e, 'source': s, 'rating': random.randrange(1,10) } for s in random.sample(sources, random.randrange(1, len(sources))) ]
			rating = mean(map(lambda a: a['rating'], local))
			for r in local: put(e, r['source'], r['rating'])
			ratings += local
			i = i + len(local)

	# Ha. Thanks to Edward for the fix on this one.
	for e,rating in { k: mean({r['source']: r['rating'] for r in v}.values()) for k,v in itertools.groupby(sorted(ratings, key=lambda r: r['target']), lambda r : r['target']) }.items():
		result({ 'type': 'EXPECT_RATING', 'entity': e, 'got': get(e), 'expected': rating })

	u = map(lambda a,b : a - b, usage(), us)
	result({ 'type': 'USAGE', 'result': u })
	# How much of the data store has been used
	result({ 'type': 'TOTAL', 'result': sum(u), 'limit': 0.75*n*limit - sum(us) })
	# What is the distribution like between data stores
	result({ 'type': 'STDDEV', 'result': stddev(u), 'factor': mean(u)/10.0 })
	# How many ratings did we manage to throw in
	result({ 'type': 'COUNT', 'result': i })
	



# Go through all the tests and run them
for test in tests:
	test()

# Shut. down. everything.
server.terminate()
for p in processes: p.terminate()

# Fin.
