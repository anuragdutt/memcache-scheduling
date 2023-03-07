import os
import sys
import random
import numpy as np
import pickle
import string

random.seed(12345)
np.random.seed(12345)

if __name__ == "__main__":

	with open('/home/pace-admin/memcache-scheduling/data/memcache_hashmap_size.pkl', 'rb') as file:
		# Deserialize the dictionary using pickle.load()
		call_size = pickle.load(file)
	file.close()

	count = len(call_size)
	print("total files are : ", count)

	p = 0.005
	print("starting index generation")
	call_list = []
	for i in range(1000000):
		repeat = 1
		while repeat == 1:
			call_key = int(random.expovariate(p))
			if call_key <= count-1:
				repeat = 0
		call_list.append(call_key+1)
	print("ending index generation")



	print("Dumping the geometric call list")
	with open('/home/pace-admin/memcache-scheduling/data/call_list_main.pkl', 'wb') as call_file:
    	# Serialize the dictionary using pickle.dump()
		pickle.dump(call_list, call_file)

	call_file.close()

	max_samples = 100

	for s in range(max_samples):
		random.seed(s+1)
		label = s + 1
		cl = random.sample(call_list, 1000)
		fn = "/home/pace-admin/memcache-scheduling/data/sampled_call_lists/call_list_sample_" + str(label) + ".pkl"
		with open(fn, 'wb') as call_file:
    		# Serialize the dictionary using pickle.dump()
			pickle.dump(cl, call_file)
		fn.close()
