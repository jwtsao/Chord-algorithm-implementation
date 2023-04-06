#!/usr/bin/env python3.9

import msgpackrpc
import time

def new_client(ip, port):
	return msgpackrpc.Client(msgpackrpc.Address(ip, port))

client_1 = new_client("127.0.0.1", 5057)
client_2 = new_client("127.0.0.1", 5058)
client_3 = new_client("127.0.0.1", 5059)
client_4 = new_client("127.0.0.1", 5060)


print(client_1.call("get_info"))
print(client_2.call("get_info"))
print(client_3.call("get_info"))
print(client_4.call("get_info"))

client_1.call("create")
client_2.call("join", client_1.call("get_info"))
client_3.call("join", client_2.call("get_info"))
client_4.call("join", client_3.call("get_info"))

//test the functionality after all nodes have joined the Chord ring
print(client_1.call("find_successor", 341667072))
print(client_2.call("find_successor", 341667074))
print(client_3.call("find_successor", 162507930))
print(client_4.call("find_successor", 1796965312))
