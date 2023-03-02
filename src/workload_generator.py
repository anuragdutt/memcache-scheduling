from pymemcache.client.base import Client
import numpy as np


if __name__ == "__main__":

	np.seed(12345)
	mc = Client('localhost:11211')  # Replace with your Memcached server's address

	# Geometric distribution for the keys
	key_probs = np.power(2, -np.arange(20))
	key_probs /= np.sum(key_probs)

	# Uniform distribution for the values
	min_value_size = 10240  # 10 KB
	max_value_size = 10485760  # 10 MB

	total_size = 0
	while total_size < 8 * 1024 * 1024 * 1024:  # 8 GB
		# Generate a key with geometric distribution
		key = np.random.choice(np.arange(20), p=key_probs)
		key = f'key_{key}'

		# Generate a value with uniform distribution
		value_size = np.random.randint(min_value_size, max_value_size)
		value = np.random.bytes(value_size)

		print(key,value)

		# Execute the query and update the total size
		mc.set(key, value)
		total_size += len(value)

		# Print progress every 1000 iterations
		if total_size % 1000 == 0:
			print(f'Total size: {total_size / (1024 * 1024)} MB')
