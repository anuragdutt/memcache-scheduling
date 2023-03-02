import os
import sys
import memcache 
import pickle
from pymemcache.client.hash import HashClient
import hashlib

if __name__ == "__main__":
	# Open the pickle file in binary mode for reading
	with open('/home/pace-admin/memcache-scheduling/data/memcache_hashmap.pkl', 'rb') as file:
		# Deserialize the dictionary using pickle.load()
		hmap = pickle.load(file)
	file.close()
	
	# Define three Memcached servers with different ports
	servers = [
		'130.245.127.175:11211',
		'130.245.127.208:11211'
	]

	# Create a hash client with the servers and a hash function
	# client = HashClient(servers, hash_algorithm=hashlib.md5)
	client = HashClient(servers)
	# Set key-value pairs using the hash client
	for key, value in hmap.items():
		print(key, value)
		break
		# client.set(str(key), value)