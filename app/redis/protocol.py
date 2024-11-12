
class RedisDataType:
    string = '+'
    error = '-'
    integer = ':'
    b_string = '$'
    array = '*'
    null = '_'
    boolean = '#'
    delimeter = '\r\n'

class RedisCommandLists:
    ECHO = 'ECHO'

class RedisProtocolParser:
    def __init__(self, data: str):
        self.commands = data.split(RedisDataType.delimeter)
        self.len_command = len(self.commands)

    def _encode(self, vals: list) -> str:
        return RedisDataType.delimeter.join(vals)

    def _decode(self):
        command = self.commands[2].upper()
        args = []

        for i in range(4, self.len_command, 1):
            args.append(self.commands[i])

        return command, args

    def execute(self):
        command, args = self._decode()
        if command == RedisCommandLists.ECHO:
            return self.echo(args[0])
        
        return self.error("Invalid")

    def echo(self, val) -> str:
        resp = [f"{RedisDataType.b_string}{str(len(val))}", val, ""]

        return self._encode(resp)
    
    def error(self, message):
        resp = [f"{RedisDataType.error} {message}", ""]

        return self._encode(resp)

    

