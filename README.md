# TYDFS
Distributed file system using python &amp; ZMQ for communication between processes

run Client.py any number of time and pass integer id as an argument

run Datakeeper.py 3 times and give it id form 0 to 2

if you want to extend number of data keeper nodes you should change Conf.py DATA_KEEPER_IPs section, then you can run DataKeeper.py as much as you want

run Master.py once

the master and the nodes are multiprocessing with three processes if you want to change it change the Ports in Conf.py so it can suit the number of process

