import socket  # noqa: F401
import threading
import argparse
import configparser
from pathlib import Path
import re
import time

config = configparser.ConfigParser()

class RedisData:
    data = {}
    config = {
        "role": "master",
        "master_repl_offset": "0",
        "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
    }

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
    PX = "PX" # milliseconds
    EX = "EX" # seconds
    CONFIG = "CONFIG"
    KEYS = "KEYS"
    INFO = "INFO"

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
        try:
            command, args = self._decode()
            if command == RedisCommandLists.ECHO:
                return self.echo(args[0])
            
            if command == RedisCommandLists.PING:
                return self.ping()
            
            if command == RedisCommandLists.SET:
                (ttl_type, ttl) = (args[2], args[3]) if len(args)>2 else ("", None)
                return self.set(args[0], args[1], ttl=ttl, ttl_type=ttl_type)
            
            if command == RedisCommandLists.GET:
                return self.get(args[0])
            
            if command == RedisCommandLists.CONFIG:
                return self.config(args)
            
            if command == RedisCommandLists.KEYS:
                return self.keys(args)
            
            if command == RedisCommandLists.INFO:
                return self.info(args)
        except Exception as e:
            print("Something went wrong", e)
        
        return self.error("Invalid")

    def echo(self, val) -> str:
        resp = [f"{RedisDataType.b_string}{str(len(val))}", val, ""]

        return self._encode(resp)
    
    def error(self, message):
        resp = [f"{RedisDataType.error} {message}", ""]

        return self._encode(resp)
    
    def null(self):
        print("Not found")
        resp = [f"{RedisDataType.b_string}-1", ""]

        return self._encode(resp)
    
    def ping(self):
        resp = [f"{RedisDataType.string}PONG", ""]

        return self._encode(resp)
    
    @classmethod
    def invalidate_key(self, key):
        del RedisData.data[key]
    
    def set(self, key, val, ttl=None, ttl_type=""):
        RedisData.data[key] = val
        resp = [f"{RedisDataType.string}OK", ""]
        
        if ttl_type.upper() == RedisCommandLists.PX:
            # px in milliseconds convert to seconds
            threading.Timer(int(ttl)/1000, self.invalidate_key, args=[key]).start()
        elif ttl_type.upper() == RedisCommandLists.EX:
            threading.Timer(ttl, self.invalidate_key, args=[key]).start()

        return self._encode(resp)
    
    def get(self, key):

        val = RedisData.data.get(key, None)

        if not val:
            return self.null()
        
        resp = [f"{RedisDataType.b_string}{str(len(val))}", val, ""]

        return self._encode(resp)
    
    def config(self, args):

        key = args[1]

        if args[0].upper() == RedisCommandLists.GET:
            val = config.get('default', key)


        resp = [f"{RedisDataType.array}2", f"{RedisDataType.b_string}{len(key)}", key, f"{RedisDataType.b_string}{len(val)}", val, ""]

        return self._encode(resp)
    
    def keys(self, args):

        pattern = args[0]

        if pattern == "*":
            all_keys = list(RedisData.data.keys())

        

        resp = [f"{RedisDataType.array}{len(all_keys)}"]
        for key in all_keys:
            resp.append(f"{RedisDataType.b_string}{len(key)}")
            resp.append(key)

        resp.append('')

        return self._encode(resp)
    
    def info(self, args):
        section = None
        if args:
            section = args[0]

        if section == "replication":
            total_length = 0
            resp = ""
            for key, value in RedisData.config.items():
                total_length+=len(key)+len(value)+1+2
                resp += f"{key}:{value}\r\n"

            resp=[f"{RedisDataType.b_string}{total_length}", resp, ""]

            return self._encode(resp)

    

    
'''
A byte consists of 8 bits, and each hex digit represents 4 bits. Therefore, two hex digits 
(two characters) are needed to represent a full byte.
'''

def hex_to_decimal(hex_val):
    integer_val = int(hex_val, 16)
    binary_val = bin(integer_val)[2:]

    return binary_val

def hex_to_num(hex_val):
    return int(hex_val, 16)

def convert_to_b_endian(hex_val):
    hex_val = [hex_val[i:i+2] for i in range(0, len(hex_val), 2)]
    return "".join(hex_val[::-1])

def binary_to_num(binary_val):
    return int(binary_val, 2)

def hex_to_string(hex_val):
    bytes_val = bytes.fromhex(hex_val)
    string = bytes_val.decode('utf-8')
    return string


class RDBParser:
    DATABSE_SECTION = 'fe'
    HASH_TABLE = 'fb'
    LITTLE_ENDIAN = 'little_endian'
    BIG_ENDIAN = 'big_endian'
    STRING = 'string'
    FC = {'length': 8, "type": LITTLE_ENDIAN}
    FD = {'length': 4, "type": LITTLE_ENDIAN}
    BYTE_TO_HEX = 2

    encoding = {
        'fe': HASH_TABLE,
        '00' : STRING,
        'fc' : FC,
        'fd' : FD
    }


    def __init__(self, rdb_path):
        self.rdb_path = rdb_path
        self._read_content()
        self._get_hashtable()

    def _read_content(self):
        with open(self.rdb_path, 'rb') as rdb:
            self.data = rdb.read().hex()

    def _get_hashtable(self):
        '''
        Includes only hash_table contents
        '''
        self.hash_table = re.search(r'fb[a-zA-Z0-9]*ff', self.data).group()[2:-2]
        return self.hash_table
    
    def get_total_keys(self):
        pass

    def size_deccoding(self):
        '''
        The first 2 bits are imp
        if first 2 bit is 00 , then next 6 bit are size
        if first 2 bit is 01 then next 14 bits are size
        if first 2 bit is 10 then neglect 6it of 1st byte and consider rest 4 byte as number
        '''
        pass

    def string_decoding(self):
        '''
        String encoding starts with size encoding
        '''
        pass

    def read(self):
        pointer = 0
        all_keys_data = []

        num_keys = self.hash_table[pointer: pointer+2]
        pointer+=2
        num_keys_with_expiry = self.hash_table[pointer: pointer+2]
        pointer+=2

        print(f"There are total of {num_keys} keys with {num_keys_with_expiry} has expiry")        

        while pointer <= len(self.hash_table)-2:
            command = self.hash_table[pointer:pointer+2]
            pointer+=2

            temp = {}

            expiry = None
            expiry_type = None

            if command == 'fc':
                expiry = self.hash_table[pointer:pointer+self.FC['length']*2]
                pointer = pointer+self.FC['length']*2
                expiry_type = 'PX'

                command = '00'
                pointer+=2
                
            if command == 'fd':
                expiry = self.hash_table[pointer:pointer+self.FD['length']*2]
                pointer = pointer+self.FD['length']*2
                expiry_type = 'EX'

                command = '00'
                pointer+=2

            if command == '00':

                size_to_read = hex_to_num(self.hash_table[pointer:pointer+2])
                pointer+=2
                key = hex_to_string(self.hash_table[pointer:pointer+size_to_read*2])
                pointer=pointer+size_to_read*2

                size_to_read = hex_to_num(self.hash_table[pointer:pointer+2])
                pointer+=2
                val = hex_to_string(self.hash_table[pointer:pointer+size_to_read*2])
                pointer=pointer+size_to_read*2
                temp[key] = {
                    'value': val, 
                    "expiry": expiry, 
                    "expiry_type": expiry_type
                    }

            all_keys_data.append(temp)
            del temp
        
        return all_keys_data



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


def main(port):
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", port), reuse_port=True)

    while True:
        connection_object, addr = server_socket.accept() # wait for client
        print("Client", addr, "has connected")
        print("Waiting for request")
        threading.Thread(target=concurrent_request, args=[connection_object, addr]).start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='BYOR args')
    parser.add_argument('--dir', help="file path of config file")
    parser.add_argument("--dbfilename", help="filename of the config")
    parser.add_argument("--port", help="filename of the config", type=int, default=6379)
    parser.add_argument("--replicaof", type=str)

    args = parser.parse_args()
    
    config.add_section('default')

    dir = args.dir
    db_file = args.dbfilename
    port = args.port
    replicaof = args.replicaof

    if dir:
        config.set('default', 'dir', dir)

    if db_file:
        config.set('default', 'dbfilename', db_file)

    if replicaof:
        replica_ip, replica_port = replicaof.split(" ")

        PING_COMMAND = "*1\r\n$4\r\nPING\r\n"
        REPLCONF_1=f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(port))}\r\n{port}\r\n"
        REPLCONF_2="*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"

        all_comands = [PING_COMMAND, REPLCONF_1, REPLCONF_2]

        # Create a socket object (IPv4, TCP)
        replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect to the server
        replica_socket.connect((replica_ip, int(replica_port)))

        for command in all_comands:
            replica_socket.sendall(command.encode())
            replica_respone = replica_socket.recv(2046)

        RedisData.config['role'] = 'slave'


    if dir and db_file:
        path = Path(dir) / db_file

        if Path(path).exists():
            rdb = RDBParser(path)
            rdb._get_hashtable()
            data = rdb.read()
            for item in data:
                for key, val in item.items():
                    RedisData.data[key] = val['value']
                    
                    if not val['expiry']:
                        continue

                    # time is in little-endian
                    big_endian_hex_val = convert_to_b_endian(val['expiry'])
                    unix_time_stamp = hex_to_num(big_endian_hex_val)
                    if val['expiry_type'] == RedisCommandLists.PX:
                        in_seconds_unix = unix_time_stamp/1000
                        in_seconds = int(in_seconds_unix - time.time())
                    elif val['expiry_type']  == RedisCommandLists.EX:
                        in_seconds = int(unix_time_stamp - time.time())

                    if in_seconds > 0 :
                        threading.Timer(in_seconds, RedisProtocolParser.invalidate_key, args=[key]).start()
                    else:
                        del RedisData.data[key]

    main(port)
