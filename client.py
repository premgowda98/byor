import socket

# Define the server address and port (must match the server settings)
HOST = 'localhost'  # or use '127.0.0.1'
PORT = 6379        # Port number must match the server port

# Create a socket object (IPv4, TCP)
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect to the server
client_socket.connect((HOST, PORT))

messages = [
    f"*3\r\n$3\r\nSET\r\n$10\r\nstrawberry\r\n$5\r\ngrape\r\n", # set strawberry grape
    f"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", # echo hey
    f"*1\r\n$4\r\nPING\r\n", # ping
    f"*2\r\n$3\r\nGET\r\n$10\r\nstrawberry\r\n", # get strawberry
    f"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n", # get name
    f"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nprem\r\n", # set name prem
    f"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n", # get name
    # f"*3\r\n$3\r\nSET\r\n$7\r\nsession\r\n$12\r\nertayrytjyfj\r\n$2\r\nPX\r\n$4\r\n9000\r\n", # set session for 5s
    f"*2\r\n$3\r\nGET\r\n$7\r\nsession\r\n", # get name
    f"*3\r\n$5\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n", # config get dir
    f"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", # get foo
    f"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n", 
]

for message in messages:
    # Send a request message to the server
    client_socket.sendall(message.encode())

    # Receive the response from the server
    response = client_socket.recv(2046)
    print(f"Received from server: {response}")

# Close the client connection
client_socket.close()
