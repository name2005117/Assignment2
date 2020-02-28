#!/usr/bin/python
# encoding: utf-8
import zmq
import time
import random
# import kazoo
from kazoo.client import KazooState
from kazoo.client import KazooClient

log_file = './Output/broker.log'
#self, zk_server, my_address, xsub_port, xpub_port
class broker_lib:
    
    def __init__(self, id, zk_server, ip, xpub, xsub):

        self.ID = id
        self.IP = ip
        self.pubsocket = self.bindp(ip, xpub)
        self.subsocket = self.binds(ip, xsub)
        self.syncsocket = None
        self.pubsynsocket = None
        zk_server = zk_server + ':2181'
        self.zk = KazooClient(hosts=zk_server)
        self.leader_flag = False

        # the publisher and the info they store
        self.pubdict = {}
        self.publisher = {} 
        self.subdict = {}
        self.subscriber = {}
        self.init_zk()

    
    print('\n************************************\n')
    print('Init MyBroker succeed.')
    print('\n************************************\n')
    
    with open(log_file, 'w') as logfile:
        logfile.write('Init Broker succeed \n')
    
    def bindp(self,ip, port):
        # we use REQ and REP to manage between publisher and broker
        context = zmq.Context()
        p_socket = context.socket(zmq.REP)
        p_socket.setsockopt(zmq.RCVTIMEO, 3000)
        p_socket.setsockopt(zmq.SNDTIMEO, 3000)

        p_socket.bind('tcp://*:'+ port)
        with open(log_file, 'a') as logfile:
            logfile.write('5555! \n')
        return p_socket
    
    def binds(self, ip, port):
        # we use PUB and SUB to manage sub and broker
        context = zmq.Context()
        s_socket = context.socket(zmq.REP)
        s_socket.bind('tcp://*:' + port)
        with open(log_file, 'a') as logfile:
            logfile.write('5556! \n')
        
        return s_socket
    
    def init_zk(self):
        if self.zk.state != KazooState.CONNECTED:
            self.zk.start()
        while self.zk.state != KazooState.CONNECTED:
            pass 
        # wait until the zookeeper starts
        
        print('Broker %s connected to ZooKeeper server.' % self.ID)
        
        # Create the path /Brokers
        if self.zk.exists('/Brokers') is None:
            self.zk.create(path='/Brokers', value=b'', ephemeral=False, makepath=True)
        while self.zk.exists('/Brokers') is None:
            pass

        # create my node under the path /Brokers
        znode_path = '/Brokers/' + self.ID
        self.zk.create(path=znode_path, value=b'', ephemeral=True, makepath=True)
        while self.zk.exists(znode_path) is None:
            pass
        print('Broker %s created a znode in ZooKeeper server.' % self.ID)
        

    def watch_mode(self):
        print("In watch mode")
        election_path = '/Brokers/'
        leader_path = '/Leader'

        @self.zk.DataWatch(path=leader_path) 
        def watch_leader(data, state):

            if self.zk.exists(path=leader_path) is None:

                # time.sleep(random.randint(0, 3))
                election = self.zk.Election(election_path, self.ID)
                election.run(self.win_election)
    
    def win_election(self):
        print("Win election")
        leader_path = '/Leader'
        if self.zk.exists(path=leader_path) is None:
            try:
                self.zk.create(leader_path, value=self.IP.encode('utf-8'), ephemeral=True, makepath=True)
            except Exception as ex:
                print("shouldn't elect")

        while self.zk.exists(path=leader_path) is None:
            pass

        self.leader_flag = True
        #self.start_broker()
        #self.syncsocket = None
        self.pubsyncsocket = self.sourcepush('5557')
        if self.pubsyncsocket != None:
            print('Broker %s started sending msg' % self.ID)

    def pull_msg(self, address, port):
        context = zmq.Context()
        socket = context.socket(zmq.PULL)
        socket.setsockopt(zmq.RCVTIMEO, 30000)
        socket.connect('tcp://' + address + ':' + port)
        return socket

    # Leader push msg to followers
    def sourcepush(self, port):
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.bind("tcp://*" + ':' + port)
        socket.setsockopt(zmq.SNDTIMEO, 3000)
        socket.setsockopt(zmq.RCVTIMEO, 3000)
        return socket



