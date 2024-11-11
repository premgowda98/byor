import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection_object, addr = server_socket.accept() # wait for client

    while True:
        print("Client", addr, "has connected")
        print("Waiting for request")
        data = connection_object.recv(2046)
        if not data:
            print("Client Disconnected")
            break

        print("Client data" ,data)
        connection_object.sendall(b"+PONG\r\n")
        print("Request Processed")

    connection_object.close()


if __name__ == "__main__":
    main()
