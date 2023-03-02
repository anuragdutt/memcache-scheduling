import os
import sys
import random
import numpy as np
import pickle

if __name__ == "__main__":

	# Define the range for the uniform distribution
	min_value_size = 10240  # 10 KB
	max_value_size = 10485760  # 10 MB
	random.seed(12345)
	np.random.seed(12345)
	# Define the probability parameter for the geometric distribution


	# Create the dictionary with integer keys
	hmap = {}

	# Data creation for 8GB
	max_dict_size = 8 * 1024 * 1024 * 1024
	total_size = 0

	count = 1
	while total_size <= max_dict_size :  # 8 GB
		# get a random byte size
		value_size = np.random.randint(min_value_size, max_value_size)
		# get an integer corresponding to a 
		value = random.getrandbits(value_size)
		hmap[count] = value
		count += 1
		total_size += value_size

	print(count, total_size)


	p = 0.006
	call_list = []
	for i in range(100000):
		repeat = 1
		while repeat == 1:
			call_key = int(random.expovariate(p))
			if call_key <= count:
				repeat = 0
		call_list.append(call_key)

	print("Dumping the memcache hashmap")
	with open('/home/pace-admin/memcache-scheduling/data/memcache_hashmap.pkl', 'wb') as dict_file:
    	# Serialize the dictionary using pickle.dump()
		pickle.dump(hmap, dict_file)

	dict_file.close()

	print("Dumping the geometric call list")
	with open('/home/pace-admin/memcache-scheduling/data/call_list.pkl', 'wb') as call_file:
    	# Serialize the dictionary using pickle.dump()
		pickle.dump(call_list, call_file)

	call_file.close()
