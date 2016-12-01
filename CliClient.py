import asyncio
import sys


def send_input():
    global q
    asyncio.async(q.put(sys.stdin.readline()))

class CliClientProtocol(asyncio.Protocol):

    def prompt(self):
        print("\nEnter: ")

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection made to {}'.format(peername))
        self.transport = transport
        self.prompt()

    def data_received(self, data):
        global q
        message = data.decode()
        print('Data received: {!r}'.format(message))
        fut = asyncio.async(q.get())
        fut.add_done_callback(self.write_reply)
        self.prompt()

    def write_reply(self, fut):
        reply = fut.result()
        if reply.split()[0] not in ["create", "delete", "read", "append"]:
            print("Invalid command\n")
            self.prompt()
        else:
            msgtype = reply.split()[0].upper()
            reply = msgtype + " " + " ".join(reply.split()[1:])  
            print('Send: {!r}'.format(reply))
            self.transport.write(reply.encode())


def main(argv):
    server_ip = argv[1]
    server_port = argv[2]
    global q
    q = asyncio.Queue()
    print("Enter: \n")
    loop = asyncio.get_event_loop()
    loop.add_reader(sys.stdin, send_input)

    coro = loop.create_connection(CliClientProtocol, server_ip, server_port)
    client = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    loop.run_until_complete(client.wait_closed())
    loop.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv))


