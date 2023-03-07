import socket

def check_network_connectivity(ip_address, port):
    # Create a TCP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Try to connect to the server
        sock.connect((ip_address, port))
        print("Network connectivity is OK")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the socket
        sock.close()

if __name__ == "__main__":

    check_network_connectivity("130.245.127.175", 11211)
    check_network_connectivity("130.245.127.208", 11211)
