import BullyAlgorithm
import Discovery
import Synchronization
from MessageFactory import MessageFactory
import asyncio


class History():
    def __init__(self, histfile=None):
        self.file = histfile
        self.history = list()
        self.lock = asyncio.Lock()


    def get(self):
        with ( yield from self.lock ):
            return self.history

    def set(self, hist):
        with ( yield from self.lock ):
            self.history = hist.get()

    def append(self ,msg):
        with ( yield from self.lock ):
           self.history.push_back(msg)


    def write_all(self):
        pass

    # Sketchy way of serializing things.....
    @classmethod
    def fromfile(histfile):
        h = History(histfile)
        with open(histfile) as f:
            for line in f:
                h.history.append(line.strip())
        return h

    @classmethod
    def fromstring(self, msg):
        h = History()
        if msg == ",":
            return h
        tmp = msg.replace(":", " ")
        tmp = msg.split(",")[1:]
        for line in tmp:
            h.history.append(line)
        return h

    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        if len(self.history) == 0:
            return ","
        s = ""
        for h in self.history:
            s += "," + h.replace(" ", ":")
        return s


class Zxid():
    def __init__(self):
        self.epoch = 0
        self.counter = 0

    @classmethod
    def fromstring(self, msg):
        z = Zxid()
        e = msg.index("E")
        c = msg.index("C")
        z.epoch = int(msg[e+1:c])
        z.counter = int(msg[c+1:])
        return z


    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        return "E" + str(self.epoch) + "C" + str(self.counter)

    def __lt__(self, other):
        return (self.epoch < other.epoch) or (self.epoch == other.epoch and self.counter < other.counter)


class Zookeeper():

    def __init__(self, router, histfile):
        self.router = router
        self.mypid = self.router.my_pid

        self.leader = None
        self.leader_lock = asyncio.Lock()
        self.phase = 0
        self.phase_lock = asyncio.Lock()

        self.history = History(histfile) # a log of transaction proposals accepted
        self.acceptedEpoch = 0 # the epoch number of the last NEWEPOCH message accepted
        self.acceptedEpoch_lock = asyncio.Lock()
        self.currentEpoch = 0  # the epoch number of the last NEWLEADER message accepted
        self.currentEpoch_lock = asyncio.Lock()
        self.lastZxid = Zxid() # zxid of the last proposal in the history

        self.bully = BullyAlgorithm.BullyAlgo(self, self.router)
        self.discovery = Discovery.Discovery(self, self.router)
        self.synch = Synchronization.SyncAlgo(self, self.router)
        # self.phase3 = Broadcast.Phase3(self, self.router)


    def recover_from_failure(self):
        self.phase = 0
        self.bully.on_recovery_from_failure()

    def recv_message(self, message, pid):
        #Msg dispatched
        msgtype = MessageFactory.get_msg_type(message)
        args = MessageFactory.get_args(message)

        # if self.discovery.started == False and self.get_phase() == 1:
        #     self.discovery.start_phase1()

        #Phase 0: Leader Election (i.e. Bully)
        # if self.phase == 0:
        if msgtype == "ELECTION":
            self.bully.on_receive_election(pid)
            return

        elif msgtype == "OK":
            self.bully.recv_ok(pid)
            return

        elif msgtype == "COORD":
            self.bully.recv_coord(pid)
            return

        #Phase 1: Discovery
        # elif self.phase == 1:
        elif msgtype == "FOLLOWERINFO":
            leader = self.is_leader()
            if leader:
                epoch = int(args[0])
                self.discovery.L_recv_follower_info(pid, epoch)
            else:
                print("DISCOVERY: bug recv follower info on follower.")
            return

        elif msgtype == "NEWEPOCH":
            if not self.is_leader():
                epoch = int(args[0])
                self.discovery.F_recv_new_epoch(epoch)
            else:
                print("DISCOVERY: bug recv new epoch on leader.")
            return

        elif msgtype == "ACKEPOCH":
            if self.is_leader():
                epoch = int(args[0])
                history = str(args[1])
                zxid = Zxid.fromstring(args[2])
                self.discovery.L_recv_ack_epoch(pid, epoch, history, zxid)
            else:
                print("DISCOVERY: bug recv ack epoch on follower.")
            return


        #phase 2: Synchronization
        if self.phase == 2:
            if msgtype == "NEWLEADER":
                if not self.is_leader():
                    epoch = int(args[0])
                    history = str(args[1])
                    self.synch.L_recv_new_leader(pid, epoch, history)
                else:
                    print("SYNCHRON: bug recv NEWLEADER on follower.")
                return

            elif msgtype == "ACKNEWLEADER":
                if not self.is_leader():
                    epoch = int(args[0])
                    history = str(args[1])
                    self.synch.F_recv_new_epoch(pid, epoch, history)
                else:
                    print("DISCOVERY: bug recv ACKNEWLEADER on leader.")

            elif msgtype == "COMMIT":
                self.synch.recv_commit()


        #phase 3: Broadcast
        elif self.phase == 3:
            pass



        #EOF
        if msgtype == "INVALID":
            #EOF
            if message == "":
                print("Lost connection to pid %d\n" % pid)
                self.router.del_peer_by_id(pid)
                if pid == self.bully.leader:
                    self.bully.initiate_election()

            else:
                print("INVALID msg received.\n")

        elif msgtype == "PID":
            #shouldn't get here; handled elsewhere now
            print("Deprecated code")
            return int(message.split()[1])
        else:
            print("msg %s fell through" % message)








    def set_leader(self, leader):
        # with (yield from self.leader_lock):
        self.leader = leader
    def get_leader(self):
        # with (yield from self.leader_lock):
        return self.leader
    def is_leader(self):
        return self.get_leader() == self.mypid
    def set_phase(self, phase):
        # with (yield from self.phase_lock):
        self.phase = phase
    def get_phase(self):
        # with (yield from self.phase_lock):
        return self.phase
    def set_acceptedEpoch(self, acceptedEpoch):
        # with (yield from self.acceptedEpoch_lock):
        self.acceptedEpoch = acceptedEpoch
    def get_acceptedEpoch(self):
        # with (yield from self.acceptedEpoch_lock):
        return self.acceptedEpoch
    def set_currentEpoch(self, currentEpoch):
        # with (yield from self.currentEpoch_lock):
        self.currentEpoch = currentEpoch
    def get_currentEpoch(self):
        # with (yield from self.currentEpoch_lock):
        return self.currentEpoch