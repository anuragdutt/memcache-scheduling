import os
import sys
import pickle
import collections
import numpy as np
import csv

if __name__ == "__main__":
	with open('/home/pace-admin/memcache-scheduling/data/call_list_main.pkl', 'rb') as file:
	# Deserialize the dictionary using pickle.load()
		calls = pickle.load(file)
		file.close()

	cmain = [c+1 for c in calls]


	freq = collections.Counter(np.sort(calls))
	sorted_keys = sorted(freq.keys())
	sorted_values = [freq[key] for key in sorted_keys]

	# Write the sorted dictionary to a CSV file
	with open('/home/pace-admin/memcache-scheduling/data/main_dist_sorted_counter.csv', mode='w', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(['Key', 'Value'])
		for key, value in zip(sorted_keys, sorted_values):
			writer.writerow([key, value])    


	max_samples = 100

	for s in range(max_samples):
		label = s + 1
		fn = "/home/pace-admin/memcache-scheduling/data/sampled_call_lists/call_list_sample_" + str(label) + ".pkl"
		
		with open(fn, 'rb') as file:
			calls_sample = pickle.load(file)
			file.close()


		freq_s = collections.Counter(np.sort(calls_sample))
		print(len(freq_s))
		sorted_keys_s = sorted(freq_s.keys())
		sorted_values_s = [freq_s[key] for key in sorted_keys_s]

		fncsv = "/home/pace-admin/memcache-scheduling/data/sampled_call_freqs/sample_dist_sorted_counter_"+ str(label) + ".csv"
		# Write the sorted dictionary to a CSV file
		with open(fncsv, mode='w', newline='') as file:
			writer = csv.writer(file)
			writer.writerow(['Key', 'Value'])
			for key, value in zip(sorted_keys_s, sorted_values_s):
				writer.writerow([key, value]) 
		file.close()   


