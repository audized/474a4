
# Get the good stuff
import redis, json, mimeparse, os, sys, hashlib
from bottle import route, run, request, response, abort

config = { 'servers': [{ 'host': 'localhost', 'port': 6379 }] }
hash_algorithm = 'sha1'

if (len(sys.argv) > 1):
	config = json.loads(sys.argv[1])

# Connect to a single Redis instance
#client = redis.StrictRedis(host=config['servers'][0]['host'], port=config['servers'][0]['port'], db=0)

# Add a route for a user updating their rating of something which can be accessed as:
# curl -XPUT -H'Content-type: application/json' -d'{ "rating": 5, "source": "charles" }' http://localhost/rating/bob
# Response is a JSON object specifying the new rating for the entity:
# { rating: 5 }
@route('/rating/<entity>', method='PUT')
def put_rating(entity):

	# Check to make sure JSON is ok
	type = mimeparse.best_match(['application/json'], request.headers.get('Accept'))
	if not type: return abort(406)

	# Check to make sure the data we're getting is JSON
	if request.headers.get('Content-Type') != 'application/json': return abort(415)

	response.headers.append('Content-Type', type)
	
	# Read in the data
	data = json.load(request.body)
	rating = data.get('rating')
	source = data.get('source')

	# Basic sanity checks on the rating
	if isinstance(rating, int): rating = float(rating)
	if not isinstance(rating, float): return abort(400)

	rating_key = entity+'/ratings'
	average_key = entity+'/average'
	client = get_redis_client(entity)
	old_rating = client.zscore(rating_key, source)
	average = client.get(average_key)

	# Update user rating and average rating for the tea only if the user's new
	# rating is not the same as his old one
	# Or add the new rating if the user has not rated the tea yet
	if not old_rating or old_rating != rating:
		total = rating
		if not average:
			average = 0.0
		total += float(average) * int(client.zcard(rating_key))
		# Take away the old rating if user has already rated the tea
		if old_rating:
			total -= float(old_rating)
		client.zadd(rating_key, rating, source)
		average = total / int(client.zcard(rating_key))
		client.set(average_key, average)
	
	# Return the new average rating for the entity
	return {
		"rating": average
	}


# Add a route for getting the aggregate rating of something which can be accesed as:
# curl -XGET http://localhost/rating/bob
# Response is a JSON object specifying the rating for the entity:
# { rating: 5 }
@route('/rating/<entity>', method='GET')
def get_rating(entity):
	client = get_redis_client(entity)
	return {
		"rating": client.get(entity+'/average')
	}

# Add a route for deleting all the rating information which can be accessed as:
# curl -XDELETE http://localhost/rating/bob
# Response is a JSON object showing the new rating for the entity (always null)
# { rating: null }
@route('/rating/<entity>', method='DELETE')
def delete_rating(entity):
	# Remove average
	client = get_redis_client(entity)
	count = client.delete(entity+'/average')
	if count == 0:
		return abort(404)
	else:
		# Remove all user ratings
		rating_key = entity+'/ratings'
		for user in client.zrange(rating_key, 0, -1):
			client.zrem(rating_key, user)
	return { "rating": None }

# Return a redis client given an entity
def get_redis_client(entity):
	# Hash the entity to a hexadecimal value and then convert the value to int
	h = hashlib.new(hash_algorithm)
	h.update(entity)
	partition = int(long(h.hexdigest(), base=16) % len(config['servers']))
	# Connect to the redis instance and return the client object
	return redis.StrictRedis(host=config['servers'][partition]['host'], port=config['servers'][partition]['port'], db=0)

# Fire the engines
if __name__ == '__main__':
	run(host='0.0.0.0', port=os.getenv('PORT', 2500), quiet=True)



