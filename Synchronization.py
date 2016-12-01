from MessageFactory import MessageFactory

class SyncAlgo():
    '''Phase 2: Synchronization'''

    def __init__(self, zookeeper, router):
        self.router = router
        self.zook = zookeeper #sneaky backreference
        self.mypid = self.router.my_pid


        self.leader = self.zook.get_leader
        self.set_phase = self.zook.set_phase
        self.set_acceptedEpoch = self.zook.set_acceptedEpoch
        self.acceptedEpoch = self.zook.get_acceptedEpoch
        self.currentEpoch = self.zook.get_currentEpoch
        self.history = self.zook.history
        self.set_history = self.zook.history.set
        self.lastZxid = self.zook.lastZxid

        self.quorum = dict()


    def goto_phase3(self):
        print("Completed phase 2!")


    def start_phase2(self):
        print("SYNC: Start Phase 2")


    def L_send_new_leader(self):
        self.router.broadcast_to_peers( MessageFactory.NEWLEADER(self.acceptedEpoch, self.history) )

    def L_recv_ack_new_leader(self, pid, acceptedEpoch, history):
        print("SYNCH: Leader received ack new leader from %d." % pid)
        self.quorum[pid] = (acceptedEpoch, history)
        if len(self.quorum) >= self.router.majority():
            self.router.broadcast_to_peers( MessageFactory.COMMIT(self,acceptedEpoch, self.history) )
            self.goto_phase3()


    def F_recv_new_leader(self, pid, acceptedEpoch, history):
        pass
        
#         upon receiving NEWLEADER(e', H) from L do
#             if F.acceptedEpoch = e' then
#                 atomically
#                     F.currentEpoch ← e' # stored to non-volatile memory
#                     for each <v, z> ∈ H, in order of zxids, do
#                         Accept the proposal <e', <v, z>>
#                     end
#                     F.history ← H # stored to non-volatile memory
#                 end
#                 Send an ACKNEWLEADER(e', H) to L
#             else
#                 F.state ← election
#                 goto Phase 0
#             end
#         end
#         upon receiving COMMIT from L do
#             for each outstanding transaction <v, z> ∈ F.history, in order of zxids, do
#                 Deliver <v, z>
#             end
#         goto Phase 3
#         end
#     '''



    def recv_ack_new_leader(self, pid, acceptedEpoch, history):
        if self.peer == "FOLLOWER":
            self.F_recv_new_epoch(pid, acceptedEpoch, history)
        else:
            print("DISCOVERY: bug recv ACKNEWLEADER on leader.")


    # def recv_commit(self, pid, currentEpoch, history, lastZxid):
    #     if self.peer == "FOLLOWER":
    #         print("DISCOVERY: bug recv ach epoch on follower.")
    #     else:
    #         self.L_recv_ack_epoch(pid, currentEpoch, history, lastZxid)