import os
import sys
from pymemcache.client.hash import HashClient
from pymemcache.client.base import Client
import threading
import time
import concurrent.futures
import pickle
import time
import signal
import random

def signal_handler(sig, frame):
    print("Interrupted by user, shutting down...")
    sys.exit(0)

def get_from_memcached(key):
	
	servers = [
		'130.245.127.175:11211',
		'130.245.127.208:11211'
	]
	client = HashClient(servers, connect_timeout = 100, timeout = 100, no_delay=True)
	start_time = time.monotonic()
	value = client.get(key)
	end_time = time.monotonic()
	elapsed_time = end_time - start_time
	client.close()
	file_size = call_size[key]
	time.sleep(0.5)

	return value, elapsed_time, file_size


threads = []

if __name__ == "__main__":
	signal.signal(signal.SIGINT, signal_handler)

	with open('/home/pace-admin/memcache-scheduling/data/memcache_hashmap_size.pkl', 'rb') as file:
		# Deserialize the dictionary using pickle.load()
		call_size = pickle.load(file)
	file.close()

	max_samples = 10

	for s in range(max_samples):
		label = s + 1
		fn = "/home/pace-admin/memcache-scheduling/data/sampled_call_lists/call_list_sample_" + str(label) + ".pkl"
		
		with open(fn, 'rb') as file:
			calls_sample = pickle.load(file)
			file.close()



		max_threads = min(20, len(calls_sample))

		timing_list = []
		values = []
		count = 0
		max_retries = 2

		timed_out_keys = []

		with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
			future_to_key = {executor.submit(get_from_memcached, str(key)): key for key in calls_sample}
			for future in concurrent.futures.as_completed(future_to_key):
				key = future_to_key[future]
				retries = 0
				count += 1
				while retries <= max_retries:
					try:
						value, elapsed_time, fs = future.result()
						values.append(value)
						timing_list.append(elapsed_time)
						if count % 50 == 0:
							print("current count of execution is : ", count)
						# print(f"Thread for key '{key}' got value in {elapsed_time:.10f} seconds")
						# print("value of key is ", key, " is : ", sys.getsizeof(value), " and the actual file size is : ", fs)
						break
					except Exception as exc:
						print(f"Thread for key '{key}' generated an exception: {exc}")
						retries += 1
					except KeyboardInterrupt:
						print("Interrupted by user, shutting down...")
						executor.shutdown(wait=False)
						sys.exit(0)

					if retries == max_retries+1:
						timed_out_keys.append(key)
						print(f"limit exceed for key '{key}' for max number of retries")

		print("count of failed threads in second iteration is : ", len(timed_out_keys))

		fn = "/home/pace-admin/memcache-scheduling/results/timing_list_default/timing_list_" + str(label) + ".pkl"
		with open(fn, 'wb') as timing_file:
    		# Serialize the dictionary using pickle.dump()
			pickle.dump(timing_list, timing_file)
		timing_file.close()
