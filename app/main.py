import socket  # noqa: F401
import threading
import time

def concurrent_request(conn_object, addr):
    while True:
        print("\nRecieved Request from addr", addr)
        data = conn_object.recv(2046)
        if not data:
            print("Client Disconnected")
            break

        print("Client data recieved" ,data)
        conn_object.sendall(b"+PONG\r\n")
        print("Request Processed\n")

    conn_object.close()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection_object, addr = server_socket.accept() # wait for client
        print("Client", addr, "has connected")
        print("Waiting for request")
        threading.Thread(target=concurrent_request, args=[connection_object, addr]).start()


if __name__ == "__main__":
    main()
