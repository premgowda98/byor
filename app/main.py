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
        "replicas_count" : 0,
        "replicas_details": {},
        "replicas_ack": {}
    }
    empty_rdp = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    sync_enabled=False
    replica_added=False
    data_read_from_master=0
    commands_buffer=[]

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
    REPLCONF = "REPLCONF"
    ACK="ACK"
    PSYNC = "PSYNC"
    WAIT = "WAIT"

class RedisProtocolParser:
    def __init__(self, data: str, conn_object, from_master=False):
        try:
            self.from_master = from_master
            self.current_len_data = len(data)
            if self.from_master:
                RedisData.data_read_from_master += self.current_len_data
            self.commands = data.split(RedisDataType.delimeter)
            self.len_command = len(self.commands)
            self.conn_object = conn_object
        except Exception as e:
            print("something went wrong", e)

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
            self.command = command

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
            
            if command == RedisCommandLists.REPLCONF:
                return self.repliaconf(args)
            
            if command == RedisCommandLists.PSYNC:
                return self.sync(args)
            
            if command == RedisCommandLists.WAIT:
                return self.wait(args)
            
        except Exception as e:
            print("Something went wrong", e)
        
        return self.error("Invalid")

    def echo(self, val) -> str:
        resp = [f"{RedisDataType.b_string}{str(len(val))}", val, ""]

        return self._encode(resp)
    
    def error(self, message):
        resp = [f"{RedisDataType.error} {message}", ""]

        return self._encode(resp)
    
    def okay(self):
        resp = [f"{RedisDataType.string}OK", ""]

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
        
        if ttl_type.upper() == RedisCommandLists.PX:
            # px in milliseconds convert to seconds
            threading.Timer(int(ttl)/1000, self.invalidate_key, args=[key]).start()
        elif ttl_type.upper() == RedisCommandLists.EX:
            threading.Timer(ttl, self.invalidate_key, args=[key]).start()

        if not self.from_master:
            return self.okay()
    
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
        
    def repliaconf(self, args):
        if args[0]=="listening-port":
            # storing the connection object of the replica
            # to comminticate later
            RedisData.config["replicas_count"] += 1
            RedisData.config["replicas_details"].update({f"replica-{RedisData.config['replicas_count']}" : self.conn_object})
            RedisData.config['replicas_ack'].update({f"replica-{RedisData.config['replicas_count']}": True})

        if args[0].upper()=="GETACK":
            total_data_read = RedisData.data_read_from_master - self.current_len_data
            resp = [f"{RedisDataType.array}3", 
                    f"{RedisDataType.b_string}{len(RedisCommandLists.REPLCONF)}",RedisCommandLists.REPLCONF,
                    f"{RedisDataType.b_string}{len(RedisCommandLists.ACK)}", RedisCommandLists.ACK,
                    f"{RedisDataType.b_string}{len(str(total_data_read))}", f"{total_data_read}", ""
            ]

            resp_to_send = self._encode(resp)
            if self.from_master:
                self.conn_object.sendall(resp_to_send.encode()) 

        return self.okay()

        
    def sync(self, args):
        resp = [f"{RedisDataType.string}FULLRESYNC {RedisData.config['master_replid']} {RedisData.config['master_repl_offset']}", ""]
        RedisData.sync_enabled=True
        return self._encode(resp)
    
    def empty_rdb_file(self):
        bytes_data = hex_to_binary(RedisData.empty_rdp)
        return f"${int(len(bytes_data))}\r\n".encode()+bytes_data
    
    def wait(self, args):
        timeout = args[1]

        if int(args[0])<=len([x for x in list(RedisData.config['replicas_ack'].values()) if x]):
            return self._encode([f"{RedisDataType.integer}{len([x for x in list(RedisData.config['replicas_ack'].values()) if x])}", ""])

        time.sleep(int(timeout)/1000)

        return self._encode([f"{RedisDataType.integer}{len([x for x in list(RedisData.config['replicas_ack'].values()) if x])}", ""])
        # return self._encode([f"{RedisDataType.integer}{RedisData.config["replicas_count"]}", ""])


        
'''
A byte consists of 8 bits, and each hex digit represents 4 bits. Therefore, two hex digits 
(two characters) are needed to represent a full byte.
'''

def hex_to_decimal(hex_val):
    integer_val = int(hex_val, 16)
    binary_val = bin(integer_val)[2:]

    return binary_val

def hex_to_binary(hex_val):
    bytes_data = bytes.fromhex(hex_val)

    return bytes_data

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
    

def handle_replica_data(name, conn_object, data):
    conn_object.sendall(data)
    if b'SET' in data:
        print(f"before set for {name} {RedisData.config['replicas_ack']}")
        RedisData.config['replicas_ack'].update({name: False})
        conn_object.sendall(b'*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n')
        print(f"after set {name} {RedisData.config['replicas_ack']}")   

    replica_response = conn_object.recv(2046)
    print(f"response from {name} is {replica_response}")
    if b'ACK' in replica_response:
        print(f"Before update for {name} {RedisData.config['replicas_ack']}")
        RedisData.config['replicas_ack'].update({name: True})
        print(f"After update {name} {RedisData.config['replicas_ack']}")

    if not replica_response:
        conn_onj = RedisData.config['replicas_details'].get(name, None)
        RedisData.config['replicas_count']-=1
        if conn_onj:
            del RedisData.config['replicas_details'][name]

    


def concurrent_request(conn_object, addr):
    while True:
        print("\nRecieved Request from addr", addr)
        data = conn_object.recv(2046)
        if not data:
            print("Client Disconnected")
            break

        rpp = RedisProtocolParser(data.decode(), conn_object)
        redis_response = rpp.execute()

        conn_object.sendall(redis_response.encode())
            
        if RedisData.sync_enabled:
            RedisData.sync_enabled = False
            RedisData.replica_added = True

            redis_response = rpp.empty_rdb_file()
            conn_object.sendall(redis_response)

        if RedisData.replica_added:
            # sending the command to all the replicas
            # using previously stored connection object

            if rpp.command in [RedisCommandLists.SET]:
                for name, conn in RedisData.config['replicas_details'].items():
                    threading.Thread(target=handle_replica_data, args=[name, conn, data], name=name).start()
          
        print(f"Request Processed for {addr}\n")

    conn_object.close()

def main(port):
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print(f"Started Redis Server on port, {port}")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", port), reuse_port=True)

    while True:
        connection_object, addr = server_socket.accept() # wait for client
        print(f"Client {addr} has sent request to redis server {port}")
        threading.Thread(target=concurrent_request, args=[connection_object, addr], name="handle-client-thread").start()

def connect_to_master(port):

    # Connect to the server
    print(f"Connecting to Redis Master on port {master_port}")

    PING_COMMAND = "*1\r\n$4\r\nPING\r\n"
    REPLCONF_1=f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(port))}\r\n{port}\r\n"
    REPLCONF_2="*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    PSYNC = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"

    all_comands = [PING_COMMAND, REPLCONF_1, REPLCONF_2]

    # Create a socket object (IPv4, TCP)
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    master_socket.connect((master_ip, int(master_port)))
    print(f"Connected to Redis Master on port {master_port}")

    for cat, command in zip(['Ping', 'Repl_1', 'Repl_2'], all_comands):
        print(f"Sending command {cat}")
        master_socket.sendall(command.encode())
        master_response = master_socket.recv(2046)

    # send PSYNC Command
    print("Sending PSync Command")
    master_socket.sendall(PSYNC.encode())
    while True:
        master_response = master_socket.recv(2046)
        if not master_response:
            print("Master Disconnected")
            break

        # check if commands are in same buffere data
        resp_commands = []
        start_idx = master_response.find(b'*')
        if start_idx>=0:
            required_response = master_response[start_idx:]
            # get all arrays
            resp_arry = required_response.split(b'*')
            add_to_next=b''
            while resp_arry:
                resp_command = resp_arry.pop()
                add_to_next = b'' or add_to_next
                if not resp_command:
                    continue
                if resp_command == b'\r\n':
                    add_to_next = b'*\r\n'
                    continue
                resp_commands.append(b'*'+resp_command+add_to_next)
                add_to_next=b''


        print(f"executing resp commands {resp_commands}")
        while resp_commands:
            try:
                rpp = RedisProtocolParser(resp_commands.pop().decode(), master_socket, from_master=True)
                rpp.execute()
            except Exception as e:
                print("Could not set values", e)
            finally:
                print("Processed data from master")


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

    # start the main redis server
    threading.Thread(target=main, args=[port], name="Main redis thread").start()

    if dir:
        config.set('default', 'dir', dir)

    if db_file:
        config.set('default', 'dbfilename', db_file)

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
                        threading.Timer(in_seconds, RedisProtocolParser.invalidate_key, args=[key], name=f"key-invalidation-{key}").start()
                    else:
                        del RedisData.data[key]
    

    if replicaof:
        RedisData.config['role'] = 'slave'

        # master connection details
        master_ip, master_port = replicaof.split(" ")

        threading.Thread(target=connect_to_master, args=[port], name="slave-thread").start()


 