import os
import sys
import time
import signal
import pickle
from pymemcache.client.hash import HashClient
from pymemcache.client.base import Client
import pandas as pd

## write a memcache call to server

def signal_handler(sig, frame):
	print("Interrupted by user, shutting down...")
	sys.exit(0)


def get_from_memcached_s1(key):
	
	# Define three Memcached servers with different ports
	server = ['130.245.127.175:11211']


	# client = HashClient(servers, connect_timeout = 100, timeout = 100, no_delay=True)
	client = HashClient(server, connect_timeout=5, timeout=5, no_delay = True)
	start_time = time.monotonic()
	value = client.get(key)
	end_time = time.monotonic()
	elapsed_time = end_time - start_time
	client.close()
	time.sleep(0.5)
		
	return value, elapsed_time


def get_from_memcached_s2(key):
	
	# Define three Memcached servers with different ports
	server = ['130.245.127.208:11211']

	# client = HashClient(servers, connect_timeout = 100, timeout = 100, no_delay=True)
	client = HashClient(server, connect_timeout=5, timeout=5, no_delay = True)
	start_time = time.monotonic()
	value = client.get(key)
	end_time = time.monotonic()
	elapsed_time = end_time - start_time
	client.close()
	time.sleep(0.5)
		
	return value, elapsed_time

def get_from_memcached_s3(key):
	
	# Define three Memcached servers with different ports
	server = ['130.245.127.153:11211']


	# client = HashClient(servers, connect_timeout = 100, timeout = 100, no_delay=True)
	client = HashClient(server, connect_timeout=5, timeout=5, no_delay = True)
	start_time = time.monotonic()
	value = client.get(key)
	end_time = time.monotonic()
	elapsed_time = end_time - start_time
	client.close()
	time.sleep(0.5)
		
	return value, elapsed_time

def get_from_memcached_s4(key):
	
	# Define three Memcached servers with different ports
	server = ['130.245.127.132:11211']
	# client = HashClient(servers, connect_timeout = 100, timeout = 100, no_delay=True)
	client = HashClient(server, connect_timeout=5, timeout=5, no_delay = True)
	start_time = time.monotonic()
	value = client.get(key)
	end_time = time.monotonic()
	elapsed_time = end_time - start_time
	client.close()
	time.sleep(0.5)
		
	return value, elapsed_time



if __name__ == "__main__":
	signal.signal(signal.SIGINT, signal_handler)

	## s1 - 15, s2 - 1, s3 - 34, s4 - 900

	max_samples = 100
	key = "15"

	run = []
	s1_time = []
	s1_value = []

	s2_time = []
	s2_value = []

	s3_time = []
	s3_value = []

	s4_time = []
	s4_value = []

	print("starting run for server 1")
	key = str(15)
	for s in range(max_samples):
		value, elapsed_time = get_from_memcached_s1(key)
		run.append(s+1)
		s1_time.append(elapsed_time)
		s1_value.append(sys.getsizeof(value))
		print(key, elapsed_time, sys.getsizeof(value))
	
	print("starting run for server 2")
	key = str(2)
	for s in range(max_samples):
		value, elapsed_time = get_from_memcached_s2(key)
		s2_time.append(elapsed_time)
		s2_value.append(sys.getsizeof(value))
		print(key, elapsed_time, sys.getsizeof(value))

	
	print("starting run for server 3")
	key = str(34)
	for s in range(max_samples):
		value, elapsed_time = get_from_memcached_s3(key)
		s3_time.append(elapsed_time)
		s3_value.append(sys.getsizeof(value))
		print(key, elapsed_time, sys.getsizeof(value))

	
	print("starting run for server 4")
	key = str(900)
	for s in range(max_samples):
		value, elapsed_time = get_from_memcached_s4(key)
		s4_time.append(elapsed_time)
		s4_value.append(sys.getsizeof(value))
		print(key, elapsed_time, sys.getsizeof(value))

	df = pd.DataFrame({'run': run, 's1_time': s1_time, 's1_value': s1_value, 
						's2_time': s2_time, 's2_value': s2_value, 
						's3_time': s3_time, 's3_value': s3_value, 
						's4_time': s4_time, 's4_value': s4_value})

	df.to_csv("/home/pace-admin/memcache-scheduling/results/baseline_test_single_test_c2_hashclient.csv", index=False)