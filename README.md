# YDFS
Yet Another Distributed File System

## SWITCH TO DEVELOPMENT BRANCH TO SEE CODE

### Team members and their contribution:
* **Rinat Babichev** – most of data node and master node code, docker swarm set up
* **Bogdan Fedotov** – most of client code and logging
* **Vasily Varenov** – replication code, readme, presentation

### Architecture diagram

![Architecture diagram](https://i.imgur.com/EtBskqu.png)

### Connection protocols
We are using REST API for connection, here is some examples of messages:

#### Ping master or datanode
`ANY_METHOD <master_address_and_port>/ping`

#### Add datanode
`POST <master_address_and_port>/datanode?port=<datanode_port>` Datanode's address is got from request information.

#### Reset filesystem
`DELETE <master_address_and_port>/filesystem`

#### Upload file
`POST <master_address_and_port>/file?filename=<files_absolute_path>` returns file info and list of datanodes to which client can upload this file. Then client calls `POST <datanode_address_and_port>/file&filename=<id>` with file inside of request

#### Download file
`GET <master_address_and_port>/file?filename=<files_absolute_path>` returns file info and list of datanodes from which client can download this file. Then client calls `GET <datanode_address_and_port>/file&filename=<id>` and datanode respondes with file as `text/plain`

#### Rename file
`PUT <master_address_and_port>/file?filename=<files_absolute_path>&destination=<new_absolute_path>` – it changes file's information inside master, while id stays same and no update on datanode needed.
