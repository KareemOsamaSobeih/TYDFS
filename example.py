import threading
import zmq
import random
import sys
import time
import datetime
import multiprocessing


# hard coded number (we agreed to choose that number)
port = "5556"

#number of data_nodes
n = 3
#list of ips related to each data_node
ips = ["127.0.0.1" , "127.0.0.2" , "127.0.0.3"]

lock = multiprocessing.Lock() 

def f(look_up_table,lock):
    for i in range(3):
        lock.acquire() 
        look_up_table[0]["t"] += 1
        lock.release() 


def ff(look_up_table,lock):
    for i in range(3):
        lock.acquire() 
        look_up_table[0]["t"] -= 1
        lock.release() 

# intialize  look up table 
manager = multiprocessing.Manager()
look_up_table = manager.list()
for i  in range(n):
    entry = manager.dict({"last_time_alive":datetime.datetime.now(),"is_alive": False,"t":1})
    look_up_table.append(entry)


p1 = multiprocessing.Process(target=f, args=(look_up_table,lock))
p2 = multiprocessing.Process(target=ff, args=(look_up_table,lock))

p1.start() 
p2.start() 

p1.join() 
p2.join() 


print(look_up_table[0]["t"])