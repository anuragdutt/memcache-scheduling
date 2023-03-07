import os
import sys
import memcache
import pickle
from pymemcache.client.hash import HashClient
from pymemcache.client.base import Client
import time

if __name__ == "__main__":
	# Open the pickle file in binary mode for reading
	with open('/home/pace-admin/memcache-scheduling/data/memcache_hashmap_string.pkl', 'rb') as file:
		# Deserialize the dictionary using pickle.load()
		hmap = pickle.load(file)
	file.close()
	

	# Define three Memcached servers with different ports
	server1 = '130.245.127.153:11211'
	server2 = '130.245.127.132:11211'

	# Create a hash client with the servers and a hash function
	# client = HashClient(servers, hash_algorithm=hashlib.md5)
	
	client2 = Client(server2, connect_timeout=5, timeout=5, no_delay = True)

	max_priority_size = 4 * 1024 * 1024 * 1024
	total_size = 0

	c1_keys = []
	c2_keys = []

	count = 0
	# Set key-value pairs using the hash client
	for key, value in hmap.items():
		count += 1

		if total_size <= max_priority_size:
			total_size += sys.getsizeof(value)
			print(key, sys.getsizeof(value)/(1024*1024), server1)
			client1 = Client(server1, connect_timeout=5, timeout=5, no_delay = True)
			try:
				client1.set(key, value)
			except ConnectionResetError:
				time.sleep(0.5)
				client1.set(key, value)
			except BrokenPipeError:
				time.sleep(0.5)
				client1.set(key, value)
		
			client1.close()
			# client_chk = Client(server1, connect_timeout=5, timeout=5, no_delay = True)
			# res = client_chk.get(key)
			# print(res)
			c1_keys.append(key)
		
		else:
			total_size += sys.getsizeof(value)
			print(key, sys.getsizeof(value)/(1024*1024), server2)
			client2 = Client(server2, connect_timeout=5, timeout=5, no_delay = True)
			try:
				client2.set(key, value)
			except ConnectionResetError:
				time.sleep(0.5)
				client2.set(key, value)
			except BrokenPipeError:
				time.sleep(0.5)
				client2.set(key, value)
	
			client2.close()
			# client_chk = Client(server2, connect_timeout=5, timeout=5, no_delay = True)
			# res = client_chk.get(key)
			# print(res)
			c2_keys.append(key)


		time.sleep(0.5)

		# if count == 100:
		# 	break

	# key = "99"
	# client_chk1 = Client(server1)
	# res = client_chk1.get(key)
	# print(res)


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