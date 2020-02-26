from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink

class Stopo(Topo):

    # we use star topology in Assignment 2 

    def build(self, pub, sub, broker):

        self.pubHosts = []
        self.subHosts = []
        self.brokerHosts = []
        self.switch = None
        self.manager = None

        print('Topology Architecture: Star Topology')
        
        # Add a switch
        self.switch = self.addSwitch('s1')
        print('Add a switch.')

        # Add broker host to the switch
        for i in range(broker):

            host = self.addHost('BRO%d' % (i+1))
            self.brokerHosts.append(host)
            
            self.addLink(self. brokerHosts[i], self.switch)
            print('Add link between broker host ' + self.brokerHosts[i] + ' and switch ' + self.switch)


        # Add publisher hosts to the switch
        for i in range(pub):
            host = self.addHost('PUB%d' % (i+1))
            self.pubHosts.append(host)
            
            self.addLink(self.pubHosts[i], self.switch)
            print('Add link between publisher host ' + self.pubHosts[i] + ' and switch ' + self.switch)

        # Add subscriber hosts to the switch
        for i in range(sub):
            host = self.addHost('SUB%d' % (i+1))
            self.subHosts.append(host)
            
            self.addLink(self.subHosts[i], self.switch)
            print('Add link between subscriber host ' + self.subHosts[i] + ' and switch ' + self.switch)

        
        # we also need a manager to detect the fault and do the replica manager
        
        self.manager = self.addHost('Manager')
        print('Add manager host')

        
        self.addLink(self.manager, self.switch)
        print('Add link between switch and manager host')
