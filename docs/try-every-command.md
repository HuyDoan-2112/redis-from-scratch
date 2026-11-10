# Try every command

A hands-on tour of Mini Redis with `redis-cli`. Each block shows what to type
and what you should see. It takes about 15 minutes, and covers every command,
the error replies, snapshots, save points, the AOF, and restarts.

## Setup

You need Python 3.11 or newer on Linux, macOS or WSL, and `redis-cli`
(`sudo apt install redis-tools` on Ubuntu).

Keep the data files out of the project folder:

```bash
mkdir -p /tmp/mini-redis
```

**Terminal 1** runs the server. Start it from the project root:

```bash
python3 -m mini_redis --rdb-path /tmp/mini-redis/dump.rdb
```

```text
Server listening on 127.0.0.1:6379
```

**Terminal 2** runs the commands below. The server prints one
`[+] Accepted connection` line for each `redis-cli` call. That is expected.

Sections 1 to 5 run in order on this one server, which starts empty. Numbers
from `TTL` can be one lower than shown if a second passes while you type.

To type commands without the `redis-cli` prefix, run `redis-cli` on its own to
get an interactive prompt.

## 1. Connection

```console
$ redis-cli PING
PONG
$ redis-cli PING hello
"hello"
$ redis-cli ECHO "hello world"
"hello world"
```

## 2. Strings

`SET` and `GET`, with the `NX`, `XX` and `GET` options:

```console
$ redis-cli SET name alice
OK
$ redis-cli GET name
"alice"
$ redis-cli GET missing
(nil)
$ redis-cli SET name bob NX          # only if the key does not exist
(nil)
$ redis-cli SET name bob XX          # only if the key exists
OK
$ redis-cli SET newkey v XX
(nil)
$ redis-cli SET name carol GET       # set, and return the old value
"bob"
$ redis-cli GET name
"carol"
```

`INCR` creates a missing key at 0, and refuses a value that is not an integer:

```console
$ redis-cli INCR visits
(integer) 1
$ redis-cli INCR visits
(integer) 2
$ redis-cli SET counter 41
OK
$ redis-cli INCR counter
(integer) 42
$ redis-cli INCR name
(error) ERR value is not an integer or out of range
```

Expiry options on `SET`: `EX` seconds, `PX` milliseconds, `EXAT` and `PXAT` an
absolute Unix time, and `KEEPTTL`:

```console
$ redis-cli SET session abc EX 60
OK
$ redis-cli TTL session
(integer) 60
$ redis-cli SET session abc PX 1500
OK
$ redis-cli TTL session
(integer) 1
$ redis-cli SET session abc EXAT $(( $(date +%s) + 100 ))
OK
$ redis-cli TTL session
(integer) 99
$ redis-cli SET session xyz KEEPTTL  # new value, same TTL
OK
$ redis-cli TTL session
(integer) 99
$ redis-cli GET session
"xyz"
$ redis-cli SET session xyz          # a plain SET removes the TTL
OK
$ redis-cli TTL session
(integer) -1
```

## 3. Hashes

`HSET` returns 1 for a new field and 0 for an update. It takes one field and
value per call.

```console
$ redis-cli HSET user:1 name alice
(integer) 1
$ redis-cli HSET user:1 city Hanoi
(integer) 1
$ redis-cli HSET user:1 city Hue
(integer) 0
$ redis-cli HGET user:1 city
"Hue"
$ redis-cli HGET user:1 missing
(nil)
$ redis-cli HGET name city
(error) WRONGTYPE Operation against a key holding the wrong kind of value
```

## 4. Keys and expiry

`EXISTS` counts each key you name, so a key named twice counts twice:

```console
$ redis-cli EXISTS name user:1 nothere
(integer) 2
$ redis-cli EXISTS name name
(integer) 2
```

`EXPIRE` and its options. `NX` sets a TTL only when there is none, `XX` only
when there is one, `GT` only if the new TTL is longer, and `LT` only if it is
shorter:

```console
$ redis-cli EXPIRE name 100
(integer) 1
$ redis-cli TTL name
(integer) 100
$ redis-cli EXPIRE name 200 NX
(integer) 0
$ redis-cli EXPIRE name 50 GT
(integer) 0
$ redis-cli EXPIRE name 500 GT
(integer) 1
$ redis-cli EXPIRE name 50 LT
(integer) 1
$ redis-cli TTL name
(integer) 50
$ redis-cli EXPIRE visits 100 XX
(integer) 0
```

`PEXPIREAT` takes an absolute time in milliseconds. `TTL` is -1 for a key with
no TTL and -2 for a missing key:

```console
$ redis-cli PEXPIREAT visits $(( ($(date +%s) + 30) * 1000 ))
(integer) 1
$ redis-cli TTL visits
(integer) 29
$ redis-cli TTL user:1
(integer) -1
$ redis-cli TTL nothere
(integer) -2
```

A TTL of 0 deletes the key at once. `DEL` returns how many keys it deleted:

```console
$ redis-cli EXPIRE name 0
(integer) 1
$ redis-cli GET name
(nil)
$ redis-cli DBSIZE
(integer) 4
$ redis-cli DEL user:1 counter nothere
(integer) 2
$ redis-cli DBSIZE
(integer) 2
```

Wait 30 seconds and `visits` is gone too, removed by active expiry even though
nobody reads it:

```console
$ redis-cli DBSIZE
(integer) 1
```

## 5. Error replies

These match real Redis word for word:

```console
$ redis-cli FOO
(error) ERR unknown command 'FOO'
$ redis-cli GET
(error) ERR wrong number of arguments for 'get' command
$ redis-cli SET k v EX notanumber
(error) ERR value is not an integer or out of range
$ redis-cli SET k v NX XX
(error) ERR syntax error
$ redis-cli EXPIRE k 10 FOO
(error) ERR Unsupported option FOO
$ redis-cli BGREWRITEAOF
(error) ERR AOF is not enabled
```

## 6. Snapshots: BGSAVE, LASTSAVE and restart

Still on the same server. `LASTSAVE` is the Unix time of the last successful
save. Until the first save, it is the time the server started.

```console
$ redis-cli SET name alice
OK
$ redis-cli HSET user:1 city Hanoi
(integer) 1
$ redis-cli SET session abc EX 300
OK
$ redis-cli LASTSAVE
(integer) 1794275480
$ redis-cli BGSAVE
Background saving started
$ redis-cli LASTSAVE
(integer) 1794275483
```

Terminal 1 shows `Background saving terminated with success`, and
`/tmp/mini-redis/dump.rdb` exists. Stop the server with Ctrl+C and start it
again with the same command. It loads the snapshot before it accepts clients:

```text
DB loaded from disk: 3 keys in 0.000 seconds
Server listening on 127.0.0.1:6379
```

```console
$ redis-cli GET name
"alice"
$ redis-cli HGET user:1 city
"Hanoi"
$ redis-cli TTL session
(integer) 299
```

The TTL kept counting while the server was stopped, because deadlines are
stored as absolute times.

### A damaged snapshot stops the server

Stop the server, keep a copy of the file, and cut its last byte off:

```bash
cp /tmp/mini-redis/dump.rdb /tmp/mini-redis/good.rdb
truncate -s -1 /tmp/mini-redis/dump.rdb
python3 -m mini_redis --rdb-path /tmp/mini-redis/dump.rdb
```

```text
Can't load the RDB file /tmp/mini-redis/dump.rdb: unexpected end of file
```

The server exits with code 1 before it opens the port. Starting empty would
hide the problem, and the next save would overwrite the file. Put the good
copy back:

```bash
mv /tmp/mini-redis/good.rdb /tmp/mini-redis/dump.rdb
```

## 7. Save points and the final save

Start with a save point of "5 seconds and at least 1 change", from an empty
snapshot:

```bash
rm -f /tmp/mini-redis/dump.rdb
python3 -m mini_redis --rdb-path /tmp/mini-redis/dump.rdb --save "5 1"
```

```console
$ redis-cli SET a 1
OK
```

About 5 seconds later, terminal 1 shows the automatic save:

```text
1 changes in 5 seconds. Saving...
Background saving terminated with success
```

Now write once more and stop the server with Ctrl+C right away, before the
next save point:

```console
$ redis-cli SET b 2
OK
```

```text
Received SIGINT, shutting down
DB saved on disk
```

With save points on, shutdown saves one last time. Start the server again and
`b` is there:

```console
$ redis-cli GET b
"2"
```

Without `--save`, there is no final save. Only an explicit `BGSAVE` writes
the file.

## 8. The append-only file

Stop the server, remove the snapshot from section 7, and start with the AOF on:

```bash
rm -f /tmp/mini-redis/dump.rdb
python3 -m mini_redis --rdb-path /tmp/mini-redis/dump.rdb --aof --aof-path /tmp/mini-redis/appendonly.aof
```

The snapshot has to go first. On the first start with the AOF on, the server
would load it into the new AOF (see the end of this section).

Every write goes into the file as a RESP command:

```console
$ redis-cli SET n 0
OK
$ redis-cli INCR n
(integer) 1
$ redis-cli INCR n
(integer) 2
$ redis-cli INCR n
(integer) 3
$ cat /tmp/mini-redis/appendonly.aof
*3
$3
SET
$1
n
$1
0
*2
$4
INCR
$1
n
...
```

`BGREWRITEAOF` replaces it with the shortest commands that rebuild the data:

```console
$ redis-cli BGREWRITEAOF
Background append only file rewriting started
$ cat /tmp/mini-redis/appendonly.aof
*3
$3
SET
$1
n
$1
3
```

Stop the server with Ctrl+C and start it again with the same command. The AOF
is replayed:

```console
$ redis-cli GET n
"3"
```

### One background job at a time

A save and a rewrite never run at once. Send three commands together, so they
all arrive before the first one finishes:

```console
$ printf 'BGSAVE\nBGREWRITEAOF\nBGSAVE\n' | redis-cli
Background saving started
Background append only file rewriting scheduled
(error) ERR Background save already in progress
```

The rewrite waits and starts once the save is done. On a small dataset the
save can finish between two commands; if the second line says `started`, run
the three commands again.

### Turning the AOF on keeps the snapshot

With the AOF on, the server loads the AOF and ignores `dump.rdb`. The one
exception is the first start with the AOF on, when the AOF file does not exist
yet. Then the server loads `dump.rdb` and writes the first AOF from it, so no
data is lost:

```bash
rm -f /tmp/mini-redis/appendonly.aof /tmp/mini-redis/dump.rdb
python3 -m mini_redis --rdb-path /tmp/mini-redis/dump.rdb
```

```console
$ redis-cli SET kept yes
OK
$ redis-cli BGSAVE
Background saving started
```

Stop the server and start it with the AOF on. The AOF file does not exist, so
the server loads `dump.rdb` and writes the first AOF from it:

```bash
python3 -m mini_redis --rdb-path /tmp/mini-redis/dump.rdb --aof --aof-path /tmp/mini-redis/appendonly.aof
```

```console
$ redis-cli GET kept
"yes"
$ cat /tmp/mini-redis/appendonly.aof
*3
$3
SET
$4
kept
$3
yes
```

## Clean up

Stop the server with Ctrl+C, then:

```bash
rm -r /tmp/mini-redis
```

## Going further

- The automated tests cover all of this and more, including crashes,
  `SIGKILL` under each fsync policy, and failing saves:
  `python3 -m unittest discover -s tests`.
- `redis-benchmark -t set,get -n 100000 -q` runs against Mini Redis unchanged.
- [docs/architecture.md](architecture.md) explains how each part works.
