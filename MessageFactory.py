class MessageFactory():
    'Construct/Destruct msg strings'
    msgtypes = {
        "CREATE", "DELETE", "READ", "APPEND",   #CMDS
        # "PHASE0", "PHASE1", "PHASE2", "PHASE3",
        "ELECTION", "OK", "COORD",              # Phase 0
        "FOLLOWERINFO", "NEWEPOCH", "ACKEPOCH", # Phase 1 (mostly)
        "NEWLEADER", "ACKNEWLEADER",            # Phase 2 (mostly)
        "COMMIT", "ACK",                        # Phase 3 (mostly)
        "LOG", "PID", "INVALID" # Other
    }

    @classmethod
    def get_msg_type(self, message):
        try:
            msgtype = message.split()[0]
            if msgtype not in MessageFactory.msgtypes:
                return "INVALID"
            return msgtype
        except Exception as e:
            return "INVALID"

    @classmethod
    def get_args(self, message):
        try:
            return message.split()[1:]
        except Exception as e:
            return "INVALID"


    @classmethod
    def PID(self, pid):
        return " ".join(["PID", str(pid)])

    #Bully msgs
    @classmethod
    def ELECTION(self):
        return "ELECTION"
    @classmethod
    def OK(self):
        return "OK"
    @classmethod
    def COORDINATOR(self):
        return "COORD"



    #Discovery msgs
    @classmethod
    def FOLLOWERINFO(self, epoch):
        return " ".join(["FOLLOWERINFO", str(epoch)])
    @classmethod
    def NEWEPOCH(self, epoch):
        return " ".join(["NEWEPOCH", str(epoch)])
    @classmethod
    def ACKEPOCH(self, epoch, history, zxid):
        return " ".join(["ACKEPOCH", str(epoch), str(history), str(zxid)])


