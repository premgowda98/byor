## How to get RDB file

1. Install redis `docker pull redis && docker run -d -p 6379:6379 redis`
2. Install redis-cli `sudo apt install redis-tools`
3. Use `redis-cli` to connect to redis
4. Set some key and values
5. Use `redis-cli save` to create snapshot of the data
6. Copy dump.db from container to host `docker cp <container-id>:<path in container> <desitnation path>`
7. By efault files are stored in `data/dump.rdb`
8. To see the contanet do `hexdump -C dump.rdb`