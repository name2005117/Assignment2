#!/usr/bin/python
# encoding: utf-8

import os              # OS level utilities
import sys
import argparse   # for command line parsing

import random
import time
import threading
import zmq
from kazoo.client import KazooState
from kazoo.client import KazooClient

class Sub_Info:

    # we define the publisher with borker_address, port and the topic it has
    
    def __init__(self, zk_server, port, topic):

        self.port = port
        self.topic = topic
        self.content = []

        zk_connect_addr = zk_server + ':2181'
        self.zk = KazooClient(zk_connect_addr)
        self.Connected = False
        self.socket = None
        self.Broker_IP = None

        # we randomly select the id for this publisher
        self.ID = str(random.randint(1, 10))
        self.init()
    
    def init(self):
        self.zk.start()
        while self.zk.state != KazooState.CONNECTED:
            pass

        # Create a Znode for this subscriber
        znode_path = '/Subscribers/' + self.ID
        self.zk.create(path=znode_path, value=b'', ephemeral=True, makepath=True)
        
        while self.zk.exists(znode_path) is None:
            pass
        
        # register this sub with leader once leader created
        leader_path = '/Leader'
        while self.zk.exists(leader_path) is None:
            pass
        
        data, state = self.zk.get(leader_path)
        self.Broker_IP = data.decode("utf-8")
        print(self.Broker_IP)
        
        if self.register_sub():
            print('Sub %s connected with leader' % self.ID)
            self.Connected = True

        # set High-level exist watcher for leader znode
        @self.zk.DataWatch(path=leader_path)
        def watch_leader(data, state):
            if state is None:
                self.Connected = False
                print('Sub %s loses connection with old leader' % self.ID)
            elif self.Connected is False:
                self.Broker_IP = data.decode("utf-8")
                self.socket = None
                if self.register_sub():
                    print('Sub %s reconnected with new leader' % self.ID)
                    self.Connected = True
                    
    
    def register_sub(self):

        print("***** register_sub *****")

        connection = "tcp://" + self.Broker_IP+ ":5556"
        

        # publisher requests to the broker 
        current = time.time()
        while (time.time() - current < 5):
            context = zmq.Context()
            self.socket = context.socket(zmq.REQ)
            self.socket.connect(connection)
        
        if self.socket is None:
            print('Connecttion failed.')
            return False
            
        else:
            print('Connecttion succeed.')
            
            message = 'reg' + '#' + self.ID + '#' 
        
            # send the message
            
            self.socket.send_string( message )
            
            recv_msg = self.socket.recv_string()
            
            print(recv_msg)

            return True




def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument('-i', '--ip', type=str, help='Self ip address')
    parser.add_argument('-z', '--zk', type=str, help='ZK address')
    # parse the args
    args = parser.parse_args ()

    return args



        #socket.close()
        

       

	# registation finished

def connection(s,t,id, file,content):
    
    current_time = time.time()
    topic = t
    print("Receiving messages on topics: %s ..." % topic)

    message = 'ask' + '#' + id + '#' + t + '#'
        
    # send the message
    time.sleep(1)
   
    

    s.send_string( message )
    #s.setsockopt_string(zmq.SUBSCRIBE,topic)
  
    try:
       
        while True:

            recv_msg = s.recv_string()
            print(recv_msg)
            #topic, msg = s.recv_multipart()
            if (recv_msg == 'Nothing')| (recv_msg == 'Connected'):
                if time.time()-current_time < 10:
                    time.sleep(1)
                    print('still waiting for the message')
                    m = 'ask' + '#' + id + '#' + t + '#'
                    s.send_string(m)
                else:
                    break
            else:
                
                
                if recv_msg not in content:
                    print('Topic: %s, msg:%s' % (topic, recv_msg))
                    current_time = time.time()
                    content.append(recv_msg)
                
                    with open(file, 'a') as log:
                        log.write('Receive from broker' + ' \n')
                        log.write('Topic: ' + topic + '\n') 
                        log.write('Content:' + recv_msg+ '\n')
                
                    m = 'ask' + '#' + id + '#' + t + '#'
                    s.send_string(m)
                else:
                    if time.time()-current_time < 20:
                        time.sleep(2)
                        m = 'ask' + '#' + id + '#' + t + '#'
                        s.send_string(m)
                        
                    else:
                        s.close()
                        break

            
    except KeyboardInterrupt:
        end = 'end' + '#' + id + '#' 
        s.send_string(end)
        s.close()
    print("Done.")


def main():
    
    args = parseCmdLineArgs()
    zk_address = args.zk
    port = '5556' 
    # the sub will connect to broker through 5556

    topics = {1:'animals', 2:'countries', 3:'foods', 4:'laptops', 5:'phones', 6:'universities'}
    #topic = topics[random.randint(1, 6)]
    topic = topics[1]

    sub = Sub_Info(zk_address, port, topic)
    # we begin to register the sub to the zookeeper
        
    
    '''
    register_sub(sub)
    time.sleep(5)

    context = zmq.Context()
    
    subsocket = context.socket(zmq.REQ)
    
    current = time.time()
    
    subsocket.connect("tcp://" + baddress + ":" + port)
    #subsocket.bind('tcp://*:'+ '1111')'''
    
    
    
    sub_logfile = './Output/' + sub.ID + '-subscriber.log'
    
    with open(sub_logfile, 'w') as log:
        log.write('ID: ' + sub.ID + '\n')
        log.write('Topic: ' + sub.topic + '\n') 
        log.write('Connection: tcp://%s:%s\n' % (sub.Broker_IP,sub.port))

    
    
    # we subscribe the topic the subs need
    #socket.subscribe(sub.topic)

    
    
    
    # we need to make it alive to receive the message from broker
    '''if sub.socket is None:
        print('Connection failed.')
        #return False
    else:
        print('Connection succeed!')
        '''
    threading.Thread(target=connection(sub.socket,sub.topic,sub.ID,sub_logfile,sub.content), args=()).start()
    
    # wait for the sub to be registered
    
    time.sleep(5)


    

if __name__ == '__main__':

    main()