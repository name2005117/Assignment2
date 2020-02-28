#!/usr/bin/python
import os              # OS level utilities
import sys
import argparse   # for command line parsing

from signal import SIGINT

import time
import threading
import subprocess

# These are all Mininet-specific
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.net import CLI
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
from mininet.util import pmonitor
from mininet.node import Controller, RemoteController
# This is our topology class created specially for Mininet
from Stopology import Stopo

zKServer = '~/Documents/apache-zookeeper-3.5.7-bin/bin/zkServer.sh'

def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-p", "--pub", type=int, default=3, help="Number of publishers, default 3")
    parser.add_argument ("-s", "--sub", type=int, default=5, help="Number of subscriber, default 3")
    parser.add_argument ("-b", "--broker", type=int, default=5, help="Number of broker, default 3")
    # parse the args
    args = parser.parse_args ()

    return args


def Test(pubHosts, subHosts, brokerHost, zookeeper):
    try:
        # first, we initate zookeeper server
        def zk_op():
            start_command = 'sudo xterm -hold -e ' + zKServer + ' start'
            zookeeper.cmd(start_command)
        
        threading.Thread(target=zk_op, args=()).start()
        time.sleep(1)
        print ('zookeeper is ready')
        print(zookeeper.IP())
        # then we set up brokers pus and subs
        
        
        # print(command)  
        for bk in brokerHost:
            print('start')
            def bk_op():
                # we send the zookeeper's IP and broker's IP
                command = 'sudo xterm -hold -e python3 broker.py'
                bk.cmd('%s -z %s -i %s' % (command, zookeeper.IP(), bk.IP()))
                    
            threading.Thread(target=bk_op, args=()).start()
            
            time.sleep(1)
        
        print ('broker is ready')
        

        for pub in pubHosts:
            
            def pub_op():
                command1 = 'sudo xterm -hold -e python3 publisher.py -z %s -i %s'%(zookeeper.IP(),pub.IP())

                # sudo python3 publisher.py <IP of zookeeper server> <publisher ID>
                pub.cmd(command1)
            
            threading.Thread(target=pub_op, args=()).start()
            
            time.sleep(1)
        
        print ('Pubs are ready')

        for sub in subHosts:
            
            def sub_op():
                command2 = 'sudo xterm -hold -e python3 subscriber.py -z %s -i %s'%(zookeeper.IP(),sub.IP())
                sub.cmd(command2)
            
            threading.Thread(target=sub_op, args=()).start()
            time.sleep(1)
        
        print ('Subs are ready')

        while True:
            pass


    except Exception as e:
        # print ('something goes wrong')
        print(e)

def main ():
    
    "Create and run the Wordcount mapreduce program in Mininet topology"
    parsed_args = parseCmdLineArgs ()
    
    # instantiate our topology
    print ('Instantiate topology')
    stopo = Stopo(pub = parsed_args.pub, sub = parsed_args.sub, broker = parsed_args.broker)

    # create the network
    print ('Instantiate network')
    net = Mininet (topo=stopo, link=TCLink)
    
    # activate the network
    print ('Activate network')
    net.start()

    # debugging purposes
    print ('Dumping host connections')
    dumpNodeConnections (net.hosts)
    
    
    
    #net.pingAll ()


    pubhosts =[]
    subhosts = []
    brokerhost = []
    zookeeper = None

    for host in net.hosts:
        if 'PUB' in host.name:
            pubhosts.append(host)
        elif 'SUB' in host.name:
            subhosts.append(host)
        elif 'BRO' in host.name:
            brokerhost.append(host)
        else:
            zookeeper = host

    Test(pubhosts, subhosts, brokerhost, zookeeper)
   
    net.stop ()

if __name__ == '__main__':
    
    main ()
