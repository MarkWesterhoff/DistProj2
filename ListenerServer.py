import asyncio
import sys
from MessageRouter import MessageRouter
from MessageFactory import MessageFactory
import Zookeeper

class ListenerServer:

    def __init__(self, server_pid, nodes, loop):
        self.router = MessageRouter(server_pid)

        self.server_pid = server_pid
        self.port = nodes[server_pid]["port"]
        self.cliport = nodes[server_pid]["cliport"]
        self.histfile = nodes[server_pid]["histfile"]
        self.nodes = nodes

        self.files = dict() # name -> string 

        self.zoo = Zookeeper.Zookeeper(self.router, self.histfile)
        

        # Async listeners
        self.server = loop.run_until_complete(
                asyncio.start_server(
                    self.accept_peer_connection, "", self.port, loop=loop))
        self.cliserver = loop.run_until_complete(
                asyncio.start_server(
                    self.accept_cli_connection, "", self.cliport, loop=loop))
        loop.run_until_complete(self.connect_to_peers())


    @asyncio.coroutine
    def connect_to_peers(self):
        for pid, node in self.nodes.items():
            if pid == self.server_pid:
                continue
            if not self.router.is_connected(pid):
                try:
                    reader, writer = yield from asyncio.open_connection(node["ip"], node["port"])
                    writer.write(MessageFactory.PID(str(self.server_pid) + "\n").encode("utf-8"))
                    print("Connected to PID %d\n" % pid)
                    self.router.add_peer(pid, reader, writer)
                    self.zoo.recover_from_failure()
                    asyncio.ensure_future( self.handle_peer_connection(reader, writer) )
                except Exception as e:
                    print(e)
                    pass


    @asyncio.coroutine
    def handle_peer_connection(self, reader, writer):
        while True:
            data = (yield from reader.readline()).decode("utf-8")
            print("RECEIVED %s\n" % data)
            self.zoo.recv_message(data, self.router.get_pid(reader))
            if not data:
                return None


    @asyncio.coroutine
    def accept_peer_connection(self, reader, writer):
        print("Accepted peer connection\n")
        #if checks should probs be here instead
        data = (yield from reader.readline()).decode("utf-8")
        print("recvd PID %s\n" % data)
        pid = int(data.split()[1])

        self.router.add_peer(pid, reader, writer)
        #Connected
        yield from self.handle_peer_connection(reader, writer)
        #disconnected
        yield from writer.drain()


    @asyncio.coroutine
    def handle_cli_connection(self, reader, writer):
        while True:
            data = (yield from reader.readline()).decode("utf-8")

            print("received %s" % data)
            # self.recv_cli_message(data)
            self.router.broadcast_to_cli("received %s" % data)
            self.router.broadcast_to_peers("HEARTBEAT")
            if not data:
                self.router.del_cli(reader)
                return None


    @asyncio.coroutine
    def accept_cli_connection(self, reader, writer):
        writer.write(("Welcome to server " + str(self.server_pid) + "\n").encode("utf-8"))
        self.router.add_cli(reader, writer)
        #Connected
        yield from self.handle_cli_connection(reader, writer)
        #Disconnected
        yield from writer.drain()





def main(argv):
    #parse command line args
    mypid = int(sys.argv[1])
    #parse nodes file
    nodes = dict()
    with open("nodes.txt") as f:
        for line in f:
            pid, ip, port, cliport, histfile = line.split()
            pid = int(pid)
            nodes[pid] = {"ip":ip, "port":int(port), "cliport":int(cliport), "histfile":histfile }
            #might need to mess w own pid stuff 
    # nodes = {
    #     1 : {"ip":"127.0.0.1", "port":10001, "cliport":50001},
    #     2 : {"ip":"127.0.0.1", "port":10002, "cliport":50002},
    #     3 : {"ip":"127.0.0.3", "port":10003, "cliport":50003}
    # }
    loop = asyncio.get_event_loop()
    server = ListenerServer(mypid, nodes, loop)
    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
