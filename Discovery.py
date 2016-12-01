from MessageFactory import MessageFactory

class Discovery():
    '''Phase 1: Discovery'''

    def __init__(self, zookeeper, router):
        self.router = router
        self.mypid = self.router.my_pid
        self.zook = zookeeper #sneaky backreference


        self.leader = self.zook.get_leader
        self.is_leader = self.zook.is_leader
        self.set_phase = self.zook.set_phase
        self.set_acceptedEpoch = self.zook.set_acceptedEpoch
        self.acceptedEpoch = self.zook.get_acceptedEpoch
        self.currentEpoch = self.zook.get_currentEpoch
        self.history = self.zook.history
        self.set_history = self.zook.history.set
        self.lastZxid = self.zook.lastZxid

        self.reset()


    def reset(self):
        self.info_epochs = dict()
        self.ack_epochs = dict()
        self.quorum = set()


    def goto_phase0(self):
        print("DISCOVERY: REVERTING TO PHASE 0")
        self.zook.bully.initiate_election()

    def goto_phase2(self):
        print("Completed Phase 1!")
        self.zook.synch.start_phase2()

    def start_phase1(self):
        self.reset()
        print("Start Phase 1")
        self.set_phase(1)
        if not self.is_leader():
            self.F_send_follower_info()

    #Algo funcs from paper

    def F_send_follower_info(self):
        leader = self.leader()
        print("DISCOVERY: Sending follower info to leader %d" % leader )
        self.router.send_to_peer(MessageFactory.FOLLOWERINFO( self.acceptedEpoch() ), leader )

    def F_recv_new_epoch(self, newepoch):
        if newepoch > self.acceptedEpoch():
            print("DISCOVERY: Follower new epoch is greater.")
            self.set_acceptedEpoch( newepoch )#TODO store to nonvolatile memory??
            self.router.send_to_peer(MessageFactory.ACKEPOCH(self.currentEpoch(), self.history, self.lastZxid), self.leader() )
            self.goto_phase2()
        elif newepoch < self.acceptedEpoch():
            print("DISCOVERY: Follower new epoch is lesser.")
            self.goto_phase0()

    def L_recv_follower_info(self, pid, acceptedEpoch):
        self.info_epochs[pid] = self.acceptedEpoch()
        print("DISCOVERY: Leader received follower %d info." % pid)
        if len(self.info_epochs) >= self.router.majority() and len(self.quorum) == 0:
            self.L_recv_majority_follower_info()
        
    def L_recv_majority_follower_info(self):
        print("DISCOVERY: Leader received majority follower info.")
        #Make epoch number e' such that e' > e for all e received through FOLLOWERINFO(e)
        self.quorum = set(self.info_epochs)
        e = max(self.info_epochs.values()) + 1
        self.router.broadcast_to_peers(MessageFactory.NEWEPOCH(e))

    def L_recv_ack_epoch(self, pid, currentEpoch, history, lastZxid):
        self.ack_epochs[pid] = (currentEpoch, history, lastZxid)
        print("DISCOVERY: Leader received ACK epoch.")
        if set(self.ack_epochs.keys()).issuperset(self.quorum):
            self.L_recv_majority_ack_epoch()

    def  L_recv_majority_ack_epoch(self):
        '''
        upon receiving ACKEPOCH from all followers in Q do
            Find the follower f in Q such that for all f' ∈ Q \ {f}: 
                either f'.currentEpoch < f.currentEpoch
                or (f'.currentEpoch = f.currentEpoch) ∧ (f'.lastZxid <=z f.lastZxid)  # <= for all z
            L.history ← f.history // stored to non-volatile memory
            goto Phase 2
        end
        '''
        print("DISCOVERY: Leader received majority ACK epoch.")
        #This turned out ugly; oh well
        good = True
        for outer in self.quorum:
            good = True
            for inner in self.quorum:
                if outer == inner:
                    continue
                if (self.ack_epochs[outer][0] < self.ack_epochs[inner][0]) and (self.ack_epochs[outer][2] < self.ack_epochs[inner][2]):
                    continue
                else:
                    good = False
                    break
            if good:
                self.set_history( self.ack_epochs[good] )
                self.goto_phase2()
                break