import socket

# Define the server address and port (must match the server settings)
HOST = 'localhost'  # or use '127.0.0.1'
PORT = 6379        # Port number must match the server port

# Create a socket object (IPv4, TCP)
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect to the server
client_socket.connect((HOST, PORT))

for i in range(1,3):
    # Send a request message to the server
    message = f"Message No: {i}"
    client_socket.sendall(message.encode())

    # Receive the response from the server
    response = client_socket.recv(1024)
    print(f"Received from server: {response.decode()}")

# Close the client connection
client_socket.close()
