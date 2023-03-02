import os
import sys
from pymemcache.client.hash import HashClient

if __name_ == "__main__":
	
	with open('/home/pace-admin/memcache-scheduling/data/call_list.pkl', 'rb') as file:
		# Deserialize the dictionary using pickle.load()
		hmap = pickle.load(file)
	file.close()

	servers = [
		'130.245.127.175:11211',
		'130.245.127.208:11211'
	]

	client = HashClient(servers)

	for c in call_list:
		client.get(str(c))
