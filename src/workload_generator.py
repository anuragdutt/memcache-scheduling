import os
import sys
import random
import numpy as np
import pickle
import string

random.seed(12345)
np.random.seed(12345)

def generate_random_string(byte_size):
    """
    Generate a random string of alphabets of the given byte size.
    """
    # Calculate the number of alphabets required to get the desired byte size
    num_chars = byte_size  # Two characters per byte

    # Generate a sequence of random alphabets
    return ''.join(random.choice(string.ascii_letters) for _ in range(num_chars))

if __name__ == "__main__":

	# Define the range for the uniform distribution
	min_value_size = 10240  # 10 KB
	max_value_size = 10485760  # 10 MB

	# Define the probability parameter for the geometric distribution


	# Create the dictionary with integer keys
	hmap = {}
	hsize = {}
	hstr = {}

	# Data creation for 8GB
	max_dict_size = 8 * 1024 * 1024 * 1024
	total_size = 0

	count = 1
	print("starting workload generation")
	while total_size <= max_dict_size :  # 8 GB
		# get a random byte size
		value_size = np.random.randint(min_value_size, max_value_size)
		# get an integer corresponding to a 
		value = random.getrandbits(value_size)
		value_str = generate_random_string(value_size)

		inx = str(count)
		print(inx, value_size)
		hmap[inx] = value
		hsize[inx] = value_size
		hstr[inx] = value_str
		
		count += 1
		total_size += value_size

	print(count, total_size/(1024 * 1024 * 1024))


	print("Dumping the memcache hashmap")
	with open('/home/pace-admin/memcache-scheduling/data/memcache_hashmap.pkl', 'wb') as dict_file:
		# Serialize the dictionary using pickle.dump()
		pickle.dump(hmap, dict_file)

	dict_file.close()


	print("Dumping the memcache hashmap string")
	with open('/home/pace-admin/memcache-scheduling/data/memcache_hashmap_string.pkl', 'wb') as str_file:
    	# Serialize the dictionary using pickle.dump()
		pickle.dump(hstr, str_file)

	str_file.close()

	print("Dumping the memcache hashmap size")
	with open('/home/pace-admin/memcache-scheduling/data/memcache_hashmap_size.pkl', 'wb') as size_file:
    	# Serialize the dictionary using pickle.dump()
		pickle.dump(hsize, size_file)

	size_file.close()

