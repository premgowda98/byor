import socket  # noqa: F401
import threading

class RedisData:
    data = {}

class RedisDataType:
    string = "+"
    error = "-"
    integer = ":"
    b_string = "$"
    array = "*"
    null = "_"
    boolean = "#"
    delimeter = "\r\n"

class RedisCommandLists:
    ECHO = "ECHO"
    PING = "PING"
    SET = "SET"
    GET = "GET"

class RedisProtocolParser:
    def __init__(self, data: str):
        self.commands = data.split(RedisDataType.delimeter)
        self.len_command = len(self.commands)

    def _encode(self, vals: list) -> str:
        return RedisDataType.delimeter.join(vals)

    def _decode(self):
        command = self.commands[2].upper()
        args = []

        for i in range(4, self.len_command, 2):
            args.append(self.commands[i])

        return command, args

    def execute(self):
        command, args = self._decode()
        if command == RedisCommandLists.ECHO:
            return self.echo(args[0])
        
        if command == RedisCommandLists.PING:
            return self.ping()
        
        if command == RedisCommandLists.SET:
            return self.set(args[0], args[1])
        
        if command == RedisCommandLists.GET:
            return self.get(args[0])
        
        return self.error("Invalid")

    def echo(self, val) -> str:
        resp = [f"{RedisDataType.b_string}{str(len(val))}", val, ""]

        return self._encode(resp)
    
    def error(self, message):
        resp = [f"{RedisDataType.error} {message}", ""]

        return self._encode(resp)
    
    def null(self):
        resp = [f"{RedisDataType.b_string}-1", ""]

        return self._encode(resp)
    
    def ping(self):
        resp = [f"{RedisDataType.string}PONG", ""]

        return self._encode(resp)
    
    def set(self, key, val):
        RedisData.data[key] = val
        resp = [f"{RedisDataType.string}OK", ""]

        return self._encode(resp)
    
    def get(self, key):

        val = RedisData.data.get(key, None)

        if not val:
            return self.null()
        
        resp = [f"{RedisDataType.b_string}{str(len(val))}", val, ""]

        return self._encode(resp)



def concurrent_request(conn_object, addr):
    while True:
        print("\nRecieved Request from addr", addr)
        data = conn_object.recv(2046)
        if not data:
            print("Client Disconnected")
            break

        rpp = RedisProtocolParser(data.decode())
        redis_response = rpp.execute()

        conn_object.sendall(redis_response.encode())
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
