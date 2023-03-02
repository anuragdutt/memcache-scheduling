import os
import sys
from pymemcache.client.hash import HashClient

if __name_ == "__main__":
	
	with open('/home/pace-admin/memcache-scheduling/data/call_list.pkl', 'rb') as file:
		# Deserialize the dictionary using pickle.load()
		hmap = pickle.load(file)
	file.close()

	with open('/home/pace-admin/memcache-scheduling/data/client1_call_list.pkl', 'rb') as file:
		# Deserialize the dictionary using pickle.load()
		c1_keys = pickle.load(file)
	file.close()

	with open('/home/pace-admin/memcache-scheduling/data/client2_call_list.pkl', 'rb') as file:
		# Deserialize the dictionary using pickle.load()
		c2_keys = pickle.load(file)
	file.close()

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

	for c in call_list:
		if c in c1_keys:
			res = client1.get(str(c))
		else:
			res = client2.get(str(c))

	