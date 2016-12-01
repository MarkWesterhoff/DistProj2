import asyncio
from MessageFactory import MessageFactory


class MessageRouter():

    def __init__(self, my_pid):
        self.peerlock = asyncio.Lock()
        self.peerconnections = dict()
        self.clilock = asyncio.Lock()
        self.cliconnections = dict()
        self.my_pid = my_pid

    #helper functions
    def is_connected(self, pid):
        return pid in self.peerconnections
    def get_num_peers(self):
        return len(self.peerconnections)
    def majority(self):
        return int((self.get_num_peers() + 1 + 1) / 2) # 1 for leader, 1 for math purposes


    def get_pid(self, reader):
        for pid,(r,w) in self.peerconnections.items():
            if reader is r:
                return pid
        return 0

    def add_peer(self, pid, reader, writer):
        if self.peerconnections.get(pid, None) != None:
            print("MessageRouter: Pid should already be connected.\n")
            return
        self.peerconnections[pid] = (reader, writer)
        print("Peer added: %d" % pid)

    def del_peer_by_id(self, id):
        rw = self.peerconnections.get(id,None)
        if rw == None:
            print("MessageRouter: Pid does not exist.\n")
        else:
            del self.peerconnections[id]


    def del_peer(self, reader):
        pid = self.get_pid(reader)
        if pid == None:
            print("MessageRouter: Pid does not exist.\n")
        else:
            del self.peerconnections[pid]

    def add_cli(self, reader, writer):
        if self.cliconnections.get(reader, None) != None:
            print("MessageRouter: CLI should already be connected.\n")
        self.cliconnections[reader] = (reader, writer)

    def del_cli(self, reader):
        if self.cliconnections.get(reader, None) == None:
            print("MessageRouter: CLI does not exist.\n")
        del self.cliconnections[reader]


    #send/Broadcast functions
    def send_to_peer(self, message, pid):
        writer = self.peerconnections.get(pid, None)
        if writer == None:
            print("MessageRouter: send_to_peer: Peer connection doesnt exist.\n")
        writer = writer[1]
        writer.write((message + "\n").encode("utf-8"))
        print ("Sent to pid %d: %s\n" % (pid, message))

    
    def broadcast(self, message, connections):
        for pid,(reader, writer) in connections.items():
            writer.write((message + "\n").encode("utf-8"))
            print ("mypid %d Broadcast to %s\n" % (self.my_pid, writer.get_extra_info("peername")))

    def broadcast_to_cli(self, message):
        self.broadcast(message, self.cliconnections)

    def broadcast_to_peers(self, message):
        self.broadcast(message, self.peerconnections)
        self.broadcast_to_cli(message)

    def broadcast_to_higher_pids(self, message):
        connections = {k:v for k,v in self.peerconnections.items() if k > self.my_pid}
        self.broadcast(message, connections)
        self.broadcast_to_cli(message)

    def broadcast_to_lower_pids(self, message):
        connections = {k:v for k,v in self.peerconnections.items() if k < self.my_pid}
        self.broadcast(message, connections)
        self.broadcast_to_cli(message)

    def broadcast_to_quorum(self, quorum):
        connections = {k:v for k,v in self.peerconnections.items() if k in quorum}
        self.broadcast(message, connections)
        self.broadcast_to_cli(message)


        
