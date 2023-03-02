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
	server1 = [
		'130.245.127.153:11211'
	]
	server2 = [
		'130.245.127.132:11211'
	]

	# Create a hash client with the servers and a hash function
	# client = HashClient(servers, hash_algorithm=hashlib.md5)
	client1 = HashClient(server1)
	client2 = HashClient(server2)

	max_priority_size = 4 * 1024 * 1024 * 1024
	total_size = 0

	c1_keys = []
	c2_keys = []
	# Set key-value pairs using the hash client
	for key, value in hmap.items():
		total_size += sys.getsizeof(value)
		
		if total_size <= max_priority_size:
			client1.set(str(key), value)
			c1_keys.append(key)
		else:
			client2.set(str(key), value)
			c2_keys.append(key)

	print("Dumping the client_1 call list")
	with open('/home/pace-admin/memcache-scheduling/data/client1_call_list.pkl', 'wb') as call_file:
    	# Serialize the dictionary using pickle.dump()
		pickle.dump(c1_keys, call_file)

	call_file.close()

	print("Dumping the client_2 call list")
	with open('/home/pace-admin/memcache-scheduling/data/client2_call_list.pkl', 'wb') as call_file:
    	# Serialize the dictionary using pickle.dump()
		pickle.dump(c2_keys, call_file)
	call_file.close()