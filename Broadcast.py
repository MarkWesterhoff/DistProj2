class Phase3():
    def __init__(self, zookeeper, router):
        self.router = router
        self.ZooK = zookeeper #sneaky backreference
        self.mypid = self.router.my_pid

        self.history = list()  # a log of transaction proposals accepted
        self.acceptedEpoch = 0 # the epoch number of the last NEWEPOCH message accepted
        self.currentEpoch = 0  # the epoch number of the last NEWLEADER message accepted
        self.lastZxid = Zookeeper.Zxid()   # zxid of the last proposal in the history

        self.quorum = dict()



    def L_recv_write_request(msg):
        '''
        upon receiving a write request v do
            Propose <e',<v, z>> to all followers in Q, where z = <e', c>, such that z succeeds all zxid
            values previously broadcast in e' (c is the previous zxid’s counter plus an increment of one)
        end
        '''

    def L_recv_ack():
        '''
        upon receiving ACK(<e',<v, z>>) from a quorum of followers do
            Send COMMIT(e',<v, z>) to all followers
        end
        '''


    def L_recv_follower_info():
        '''
        upon receiving FOLLOWERINFO(e) from some follower f do
            Send NEWEPOCH(e') to f
            Send NEWLEADER(e', L.history) to f
        end
        '''

    def L_recv
#     '''
#     Leader L:
#         
#         
#         // Reaction to an incoming new follower:
#         
#         upon receiving ACKNEWLEADER from follower f do
#             Send a COMMIT message to f
#             Q ← Q ∪ {f}
#         end

#     Follower F:
#         if F is leading then Invokes ready(e')
#         upon receiving proposal <e',<v, z>> from L do
#             Append proposal <e',<v, z>> to F.history
#             Send ACK(<e',<v, z>>) to L
#         end
#         upon receiving COMMIT(e',<v, z>) from L do
#             while there is some outstanding transaction <v', z'> ∈ F.history such that z' <= z do
#                 Do nothing (wait)
#             end
#             Commit (deliver) transaction <v, z>
#         end





