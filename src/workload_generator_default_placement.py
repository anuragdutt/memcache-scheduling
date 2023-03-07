import os
import sys
import pickle
from pymemcache.client.hash import HashClient
import random
import string
import time

def generate_random_string(byte_size):
    """
    Generate a random string of alphabets of the given byte size.
    """
    # Calculate the number of alphabets required to get the desired byte size
    num_chars = byte_size # Two characters per byte

    # Generate a sequence of random alphabets
    return ''.join(random.choice(string.ascii_letters) for _ in range(num_chars))


if __name__ == "__main__":
	# Open the pickle file in binary mode for reading
	with open('/home/pace-admin/memcache-scheduling/data/memcache_hashmap_string.pkl', 'rb') as file:
		# Deserialize the dictionary using pickle.load()
		hmap = pickle.load(file)
	file.close()
	
	# Define three Memcached servers with different ports
	servers = [
		'130.245.127.175:11211',
		'130.245.127.208:11211'
	]

	# servers = '130.245.127.175:11211'


	# Create a hash client with the servers and a hash function

	
	# # Set key-value pairs using the hash client
	for key, value in hmap.items():
		client = HashClient(servers, connect_timeout=5, timeout=5, no_delay = True)
		#val = generate_random_string(2048576)
		# break
		print(key, sys.getsizeof(value)/(1024*1024))
		try:
			client.set(key, value)
		except ConnectionResetError:
			time.sleep(0.5)
			client.set(key, value)
		except BrokenPipeError:
			time.sleep(0.5)
			client.set(key, value)

		client.close()
		time.sleep(0.5)
		
	