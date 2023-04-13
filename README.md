# Raft prototype

**Raft prototype** is a fault-tolerant distributed key-value store inspired by
[MIT's 6.824: Distributed System Spring 2023 Lab](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html).
The goal of the project is to build a simple, fast and reliable database on top
of Raft, a replicated state machine protocol.

## Basic info of Raft

**raft** is a replicated state machine protocol. It achieves fault tolerance by storing copies of
its data on multiple replica servers. Replication allows the service to continue operating even if
some of its servers experience failures.

## Project Status

It is still a work in progress. Here is the initial project's [proposal](PROPOSAL.md).
Other parts will be updated as soon as it is live and ready.

### Tasks

- [x] Implement Raft Consensus Algorithm
  - [x] Leader election
  - [x] Log replication
  - [x] Raft state persistent
  - [x] Snapshot system
- [ ] Build a simple client's stdin
  - [ ] Be able to specify number of servers to boot up
  - [ ] Implement GET, PUT, APPEND

### Improvements

- [ ] Enable network I/O so every server in the quorum have a public host and port, instead of just
  communicating though Go routines
  - Details:
    - net/prc doesn't have a Network object so cannot add/remove server
    - labrpc doesn't have option for network I/O (e.g.: Client.Dial and Server.ServeConn)
  - Proposed solutions (either one of these):
    - [ ] Adapt laprpc to net/rpc, add more functions and rewrite the package to use websocket
    - [ ] Use net/rpc and adapt labrpc library's functionalities
    - [ ] Keep the labrpc code, wrap it with Go net.
- [ ] Be able to start a RaftKV server one by one and watch the leader election as well as
  log replication in real time (of course with key-value service)
- [ ] Implement RESTful APIs to query each server's kv store
- [ ] Implement logging
- [ ] Make sure things are configurable
- [ ] Build CLI for server and client (e.g.: [redis demo](http://try.redis.io/))
- [ ] Make Persister read/write Raft's snapshot on/to disk (instead of holding on memory)
- [ ] How to do service discovery? (e.g.: [consul demo](https://youtu.be/huvBEB3suoo))
- [ ] Dockerize + automate build
- [ ] Continuous Integration and Delivery
- [ ] Godoc 
- [ ] Code coverage 

### Issues

- [ ] Raft log compaction logic can not pass all tests now
