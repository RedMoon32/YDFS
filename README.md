# YDFS
Yet Another Distributed File System

### Team members and their contribution:
* **Rinat Babichev** – most of data node and master node code, docker swarm set up
* **Bogdan Fedotov** – most of client code and logging
* **Vasily Varenov** – replication code, readme, presentation

### Architecture diagram

![Architecture diagram](https://i.imgur.com/EtBskqu.png)

### Connection protocols
We are using REST API for connection, here are some examples of messages:

#### Ping master or datanode
`ANY_METHOD <master_address_and_port>/ping`

#### Add datanode
`POST <master_address_and_port>/datanode?port=<datanode_port>` 

Datanode's address is got from request information.

#### Reset filesystem
`DELETE <master_address_and_port>/filesystem`

#### Upload file
`POST <master_address_and_port>/file?filename=<files_absolute_path>` 

returns file info and list of datanodes to which client can upload this file. Then client calls 

`POST <datanode_address_and_port>/file&filename=<id>` 

with file inside of request

#### Download file
`GET <master_address_and_port>/file?filename=<files_absolute_path>` 

returns file info and list of datanodes from which client can download this file. Then client calls 

`GET <datanode_address_and_port>/file&filename=<id>` 

and datanode respondes with file as `text/plain`

#### Rename file
`PUT <master_address_and_port>/file?filename=<files_absolute_path>&destination=<new_absolute_path>`

– it changes file's information inside master, while id stays same and no update on datanode needed.

### Problems we faced and solved
**1. Structure of filesystem**

We struggled a lot on how and where to store filesystem tree information. Finally, we stopped on an idea to store filesystem tree in a Master Node database and store physically files on Data Nodes just under their unique IDs. Mapping of file name to ID is stored on a Master Node.

**2. How to synchronize Master Node and Data Nodes?**

Firstly, both Master Node and Data Nodes pinged each other asking for different information. But then we found that Data Nodes do not need to ping Master. Master can periodically ask Data Nodes to check that they have all tracked files, ask if they received any new untracked files, etc. If Data Nodes do not have something they should or do not respond they are dropped.

**3. We wanted to write it on C++**

But then we decided that we also need time for life.
