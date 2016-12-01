from MessageFactory import MessageFactory
import asyncio
# TODO make this use zxids instead of pids

class BullyAlgo():

    def __init__(self, zookeeper, router):
        self.router = router
        self.mypid = self.router.my_pid
        self.zook = zookeeper # sneaky backreference
        # ugly aliases to prevent code bloat
        self.leader = self.zook.get_leader
        self.set_leader = self.zook.set_leader
        self.set_phase = self.zook.set_phase

        self.reset()

        #the wait times
        self.T0 = 0.5
        self.T1 = 0.5

    def reset(self):
        self.holdingElection = False
        self.oks = set()
        self.coords = set()

    def complete(self):
        print("BULLY: Done with Phase 0. Leader is %d" % self.leader() )
        loop = asyncio.get_event_loop()
        loop.call_soon( self.zook.discovery.start_phase1 )
        # self.zook.discovery.start_phase1()
        


    def initiate_election(self):
        print("BULLY: INITIATE ELECTION. Broadcast to higher pids")
        self.set_phase(0)
        self.reset()
        self.holdingElection = True
        self.router.broadcast_to_higher_pids( MessageFactory.ELECTION() ) #ELECTION msg
        asyncio.ensure_future( self.wait_T_for_ok_msgs(self.T0) )

    def recv_ok(self,pid):
        self.oks.add(pid)
        print("BULLY: recv ELECTION from %d" % pid)


    def wait_T_for_ok_msgs(self, T):

        yield from asyncio.sleep(T)

        print("BULLY: Done waiting for OK msgs; num recd: %d", len(self.oks))

        if len(self.oks) == 0:
            # I am LEADER
            self.set_leader( self.mypid )
            self.router.broadcast_to_lower_pids( MessageFactory.COORDINATOR() ) #COORDINATOR msg
            self.holdingElection = False
            print("BULLY: No OK responses. I am leader. Broadcasting to lower pids (if any)." )
            self.complete()
        else:
            # wait for T’ time units to receive COORDINATOR message
            asyncio.ensure_future( self.wait_T_for_coord_msgs(self.T1) )
          
    def recv_coord(self, pid):
        self.coords.add(pid)
        
        if self.mypid > pid and not self.holdingElection:
            self.initiate_election()
        else:
            print("BULLY: recv COORD from %d. Setting to leader. " % pid)
            self.set_leader( pid )
            self.holdingElection = False
            self.complete()

    def wait_T_for_coord_msgs(self, T):
        yield from asyncio.sleep(T)

        print("BULLY: Done waiting for COORD msgs; Recvd %d" % len(self.coords))
        if len(self.coords) > 1:
            print("BullyAlgo: Wait for COORD: Something went wrong; got multiple coords")
            return
        elif len(self.coords) == 1: #COORD msg recvd
            leader = self.coords.pop()
            self.set_leader( leader )
            print("BULLY: COORD msg recvd. New LEADER: %d" % leader)
            self.holdingElection = False
            self.complete()
        else:
            print("BULLY: No COORD msg recvd. Restarting Bully.")
            self.initiate_election()


    def on_receive_election(self, pid):
        print("BULLY: recv ELECTION msg from %d" % pid)
        if pid < self.mypid:
            self.router.send_to_peer(MessageFactory.OK(), pid) #ok
            if not self.holdingElection:
                self.initiate_election()


    def on_recovery_from_failure(self):
        self.initiate_election()