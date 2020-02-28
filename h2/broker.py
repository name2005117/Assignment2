#!/usr/bin/python
# encoding: utf-8
# 

import zmq
import time
import random
import argparse
import threading
import multiprocessing 
from broker_lib import broker_lib
from kazoo.client import KazooState
from kazoo.client import KazooClient

log_file = './Output/broker.log'

def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument("-i", "--ip", type=str, help='self ip address')
    #parser.add_argument("-d", "--id", type=str, help='self id')
    parser.add_argument('-z', '--zk', type=str, help='ZooKeeper address')

	# parse the args
    args = parser.parse_args()
    
    return args



def main():
    
    args = parseCmdLineArgs()
    
    ip = args.ip
    zk_address = args.zk
    #id = args.id
    id = str(random.randint(1, 10))
    print('ZooKeeper Address: ' + zk_address) 
    
    # deploy the port number for the coming request from pubs, subs and current leader
    xpub = '5555'
    xsub = '5556'
    xleader = '5557'

    # we init the broker library(my id, zk_address, my ip address,pub_port, sub_port)
     
    broker = broker_lib(id, zk_address,ip, xpub, xsub)
    
    # elect the leader under the path /Leader
    leader_path = '/Leader'

    if broker.zk.exists(leader_path):
        # If the leader znode already exists, set the flag=false, specify broker to be the follower
        broker.leader_flag = False
        broker.watch_mode()
        # this broker has to watch whether the current broker is down or not
        
        leader_address = str(broker.zk.get(leader_path)[0])
        leader_address = leader_address[2:]
        leader_address = leader_address[:-1]
        print(leader_address)
        broker.syncsocket = broker.pull_msg(leader_address, xleader)
        # sync messages from leader and update data storage
        sync_data(broker)
        
    else:
        # If the leader znode doesn't exist, set the flag=true, specify broker to be the leader

        broker.zk.create(leader_path, value=broker.IP.encode('utf-8'), ephemeral=True, makepath=True)
        while broker.zk.exists(path=leader_path) is None:
            pass
        broker.leader_flag = True
        # if this broker is elected to be the leader, then start broker
        
        # socket for leader to send sync request to followers
        #broker.pubsyncsocket = None
        broker.pubsyncsocket = broker.sourcepush(xleader)
        if broker.pubsyncsocket != None:
            print('leader: syncsocket ok')
        

    pubsocket = broker.pubsocket
    print("Bound to port 5555 and waiting for any publisher to contact\n")
    subsocket = broker.subsocket
    print("Bound to port 5556 and waiting for any subscriber to contact\n")
    
    
    # we first register all the pub and sub
    while True:
        try:
            msg1 = pubsocket.recv_string()
            print("recved msg: " + msg1)
            break
        except Exception as ex:
            print("fail to recv init msg " + str(ex))
            time.sleep(1)
    
    while broker.pubsyncsocket != None:
        
        try:
            broker.pubsyncsocket.send_string(msg1)
            print('-----')
            break
        except Exception as ex:
            print("failed to send sync " + str(ex))
            if len(broker.zk.get_children('/Brokers/')) <= 1:
                broker.pubsyncsocket = None

    
    msg11 = myRead(msg1)
    
    print("read val " + str(msg11))
    dict_update(msg11,broker.pubdict, broker.publisher)
    pubsocket.send_string('PUB-Info has been received!')
    print("sent confirm msg")
    #broker.syncsocket.send_string(msg1.encode('utf-8'))
    
    #pubsocket.send_string("Please send me the publisher \n")

    msg2 = subsocket.recv_string()

    msg22 = myRead(msg2)
    
    while broker.pubsyncsocket != None:
        print('*******')
        try:
            broker.pubsyncsocket.send_string(msg2)
            break
        except Exception as ex:
            print("failed to send sync " + str(ex))
            continue
    
    sdict_update(msg22,broker.subdict, broker.subscriber)
    subsocket.send_string('SUB-BROKER has been connected')
    #broker.syncsocket.send_string(msg2.encode('utf-8'))
    pub_thread = threading.Thread(target=pub_service, args=(broker,))
    sub_thread = threading.Thread(target=sub_service, args=(broker,))
    
    pub_thread.start()
    sub_thread.start()

    pub_thread.join()
    sub_thread.join()
    

    '''
    poller=zmq.Poller()
    poller.register(pubsocket, zmq.POLLIN)
    poller.register(subsocket, zmq.POLLIN)
    poller.register(broker.syncsocket,zmq.POLLIN)
    while True:
        
        events=dict(poller.poll())
        #print(broker.pubdict)
        if pubsocket in events:
            # how we deal with the coming publishers
            msg=read(pubsocket.recv_string())
            #broker.syncsocket.send_string(msg.encode('utf-8'))
            broker.syncsocket.send_string(pubsocket.recv_string())
            # add the coming info to the dict
            dict_update(msg, broker.pubdict, broker.publisher)
            # reply to pub that this process is done
            pubsocket.send_string('Done!!!')
        
        if subsocket in events:
            #print('we got a msg from ')

            msg = read(subsocket.recv_string())
            sdict_update(msg, broker.subdict, broker.subscriber)
            
            if msg[0] == 'ask':
                
                pub_msg = find(msg[2],broker.pubdict,broker.publisher)
                subsocket.send_string(pub_msg)
                broker.syncsocket.send_string(subsocket.recv_string())
                with open(log_file, 'a') as logfile:
                    logfile.write('Reply to SUB %s  with topic %s\n' % (msg[1], msg[2]))
            
            elif msg[0] == 'end':
                
                time.sleep(1)
                subsocket.send_string('END!!')
            
            elif msg[0] == 'reg':
                
                subsocket.send_string('Connected')'''

def pub_service(broker):
    while True:
        message = broker.pubsocket.recv_string()
        msg=myRead(message)
        #broker.syncsocket.send_string(msg.encode('utf-8'))
        if broker.pubsyncsocket is not None:
            while True:
                try:
                    broker.pubsyncsocket.send_string(message)
                    break
                except Exception as ex:
                    print("failed to send sync " + str(ex))
                    continue

        # add the coming info to the dict
        dict_update(msg, broker.pubdict, broker.publisher)
        # reply to pub that this process is done
        broker.pubsocket.send_string('Done!!!')

def sub_service(broker): 
    
    while True:
        message = broker.subsocket.recv_string()
        msg = myRead(message)
        sdict_update(msg, broker.subdict, broker.subscriber)
            
        if msg[0] == 'ask':
                
            pub_msg = find(msg[2],broker.pubdict,broker.publisher)
            
            broker.subsocket.send_string(pub_msg)
            
            broker.pubsyncsocket.send_string(message)
            
            with open(log_file, 'a') as logfile:
                logfile.write('Reply to SUB %s  with topic %s\n' % (msg[1], msg[2]))
            
        elif msg[0] == 'end':
                
            time.sleep(1)
            broker.subsocket.send_string('END!!')
            
        elif msg[0] == 'reg':
                
            broker.subsocket.send_string('Connected')              

def sync_data(broker):
    
    # we sync data from leader and update its lib
    # we only record the info about the dict and sub
    while broker.leader_flag is False:
        print('\n************************************\n')
        try:
            msg = myRead(broker.syncsocket.recv_string())
            
            # we got msg from leader
        except:
            print('Time out')
            continue
            
        print(msg)
        msg
        if msg[0]=='init':
            dict_update(msg, broker.pubdict, broker.publisher)
        elif msg[0]=='publish':
            dict_update(msg, broker.pubdict, broker.publisher)
        else:
            sdict_update(msg, broker.subdict, broker.subscriber)
        
        print(broker.pubdict)
        print('\n************************************\n')
        print('received sync msg from leader')
        # update the pub_info
        


def myRead(msg):
    info = msg.split('#')
    return info

def find(topic, dict, publisher):

    # we pick the first one to PUB
    if topic in publisher:
        owner = publisher[topic]
        length = len(dict[owner[0]][topic] )
        i = random.randint(1, length)
        content = dict[owner[0]][topic][i-1]
    else:
        content = 'Nothing'
    
    

    # when we receive the request from subs, we look up the dict to find whether there exits the info
    

    return content
    

def dict_update(msg, pubdict, publisher):
    
    if msg[0] == 'init':
        pubID = msg[1]
        topic = msg[2]
        try:
            #dict.update(topic)
            pubdict.update({pubID: {topic:[]}})
            publisher.update({topic:[]})
            #publisher[topic].update([pubID])

            with open(log_file, 'a') as logfile:
                logfile.write('PUB-Init msg: %s init with topic %s\n' % (pubID, topic))
        except KeyError:
            pass

    elif msg[0] == 'publish':
        pubID = msg[1]
        topic = msg[2]
        publish = msg[3]

        try:
            #dict[topic].update([publish])
            pubdict[pubID][topic].append(publish)
            if pubID not in publisher[topic]:
                publisher[topic].append(pubID)

            with open(log_file, 'a') as logfile:
                logfile.write('Pub No.%s published %s with topic %s\n' % (pubID, publish, topic))
        except KeyError:
            pass

def sdict_update(msg, dict, sub):
    
    if msg[0] == 'reg':
        
        subID = msg[1]
        if dict.get(subID) == None:
            dict.update({subID: []})
        #sub[topic].append(subID)
    elif msg[0] == 'ask':
        subID = msg[1]
        topic = msg[2]
        dict[subID].append(topic)
        #sub[topic].append(subID)
                
    elif msg[0] == 'end':
        subID = msg[1]
        #sub[topic].remove(subID)
        dict.pop(subID)
    
    
    

if __name__=="__main__":
	
    main()


