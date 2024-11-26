# BYOR - Custom Redis Implementation (Codecrafters Challenge)

This is a custom Redis implementation built as part of the [Codecrafters Redis Challenge](https://codecrafters.io/challenges/redis). The goal of this project is to develop a simplified version of Redis that supports basic Redis commands, including string manipulation and simple interactions with the RDB file. It is packaged as a Docker container for easy use.

## Features

Currently, this implementation supports the following features:
- **PING**: Responds with `PONG` to check if the server is alive.
- **ECHO**: Echoes back the message sent by the client.
- **SET**: Allows setting a key-value pair in the database (currently only supports strings).
- **GET**: Retrieves the value for a given key.
- **RDB file reading**: Supports reading data from an RDB file (Redis Database file).
- **Master-Slave Connection**: Implements basic master-slave replication with a simple handshake and command propagation to clients.

### Supported Data Types
- **Strings**: The only data type currently supported.

### Planned Features
- **Sets**: Support for sets will be added soon.
- **Hashes/Dicts**: Plans to implement support for dictionaries are underway.

## Getting Started

Follow these steps to build and run your own instance of My Redis:

### Prerequisites

- **Docker**: You need to have Docker installed on your system to build and run the container. You can install Docker by following the official guide: [Docker Installation](https://docs.docker.com/get-docker/).

### Building the Docker Image

To build the Docker image for this project, use the following command:

```bash
docker build -t my-redis .
```

This will create the Docker image with the tag `my-redis`.

### Running the Redis Server in a Docker Container

Once the image is built, you can run the Redis server in a Docker container using the following command:

```bash
docker run --rm --net=host my-redis
```

This will start the Redis server, allowing you to connect to it using the `redis-cli`.

### Connecting with `redis-cli`

After the Redis server is running, you can use the `redis-cli` to interact with it. For example, to set and get a value, you can do:

```bash
redis-cli
```

Then, you can issue commands like:

```bash
SET age 35
GET age
```

The Redis server will respond accordingly, for example:

```
OK
35
```

### Master-Slave Replication (Optional)

The server supports basic master-slave replication. You can connect multiple clients, and commands will be propagated to all connected clients automatically.

## Project Structure

Since the Codecrafters challenge requires the code to run in a single file, the current implementation is not yet organized into multiple folders. However, the code is modularized as much as possible within the constraints of this challenge.

## Limitations

- **Data Types**: Currently, only strings are supported. Sets, hashes, and other data types will be added in the future.
- **Persistence**: While the server can read from an RDB file, full persistence features like writing to an RDB file and AOF are not yet implemented.
- **Security**: The implementation does not include advanced security features like authentication or encryption.
  
## Contributing

Feel free to fork this project, report bugs, or submit pull requests for improvements. Contributions are welcome!