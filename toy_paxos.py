# Naive implementation of the Paxos protocol.
# Henry Robinson, 2009
# Licensed under GPL v2

# TODO:
# 1. Protocols log their state to persistent storage, and recover, rather than the fakery that's there at the moment.
# 2. No way for clients to know who the primary is
# 3. A leader that has failed and then wakes up will have no idea what the highest committed instance is. This can be helped, but the code is
# complex enough and it isn't so much an error condition as a pain.
# 4. Exercise for the reader: notify the client about the result of its request.
# 5. Garbage collect unneeded proposals, and re-propose those that seem to have stalled. Easy to do this from recvMessage.

# The idea of this Paxos implementation is to come to agreement on a
# history of values (which may represent commands in a state machine)
# We have two main players: PaxosLeader and PaxosAcceptor. PaxosLeader listens for proposals from external clients
# and runs the protocol with whichever PaxosAcceptors are currently correct.
# If a leader fails, another leader will take over once it realises. If clients fail, the protcol will still run until
# more than half have failed.
# This is designed to run all on one machine: each actor has a port number, but all are bound to localhost. Would be reasonably
# trivial to generalise this.

# See bottom of file for demonstration of use.

import threading, socket, pickle, Queue

class Message( object ):
    MSG_ACCEPTOR_AGREE = 0
    MSG_ACCEPTOR_ACCEPT = 1
    MSG_ACCEPTOR_REJECT = 2
    MSG_ACCEPTOR_UNACCEPT = 3
    MSG_ACCEPT = 4
    MSG_PROPOSE = 5
    MSG_EXT_PROPOSE = 6
    MSG_HEARTBEAT = 7
    def __init__( self, command = None ):
        self.command = command

    def copyAsReply( self, message ):
        self.proposalID, self.instanceID, self.to, self.source = message.proposalID, message.instanceID, message.source, message.to
        self.value = message.value

        
class MessagePump( threading.Thread ):
    """The MessagePump encapsulates the socket connection, and is responsible for feeding messages to its owner"""
    class MPHelper( threading.Thread ):
        """The reason for this helper class is to pull things off the socket as fast as we can, to avoid
        filling the buffer. It might have been easier to use TCP, in retrospect :)"""
        def __init__( self, owner ):
            self.owner = owner
            threading.Thread.__init__( self )
        def run( self ):
            while not self.owner.abort:
                try:
                    (bytes, addr) = self.owner.socket.recvfrom( 2048 )
                    msg = pickle.loads( bytes )
                    msg.source = addr[1]
                    self.owner.queue.put( msg )
                except:
                    pass
    
    def __init__( self, owner, port, timeout=2 ):
        self.owner = owner
        threading.Thread.__init__( self )
        self.abort = False
        self.timeout = 2
        self.port = port
        self.socket = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
        self.socket.setsockopt( socket.SOL_SOCKET, socket.SO_RCVBUF, 200000 )        
        self.socket.bind( ("localhost", port) )
        self.socket.settimeout( timeout )
        self.queue = Queue.Queue( )
        self.helper = MessagePump.MPHelper( self )
        
    def run( self ):
        self.helper.start( )
        while not self.abort:
            message = self.waitForMessage( )
            # This needs to be blocking, otherwise there's a world
            # of multi-threaded pain awaiting us            
            self.owner.recvMessage( message )

    def waitForMessage( self ):
        try:
            msg = self.queue.get( True, 3 )
            return msg
        except: # ugh, specialise the exception!
            return None

    def sendMessage( self, message ):
        bytes = pickle.dumps( message )
        address = ("localhost", message.to)
        self.socket.sendto( bytes, address )
        return True
    
    def doAbort( self ):
        self.abort = True

import random
class AdversarialMessagePump( MessagePump ):
    """The adversarial message pump randomly delays messages and delivers them in arbitrary orders"""
    def __init__( self, owner, port, timeout=2 ):
        MessagePump.__init__( self, owner, port, timeout )
        self.messages = set( )

    def waitForMessage( self ):
        try:
            msg = self.queue.get( True, 0.1 )
            self.messages.add( msg )
        except: # ugh, specialise the exception!
            pass
        if len(self.messages) > 0 and random.random( ) < 0.95: # Arbitrary!
            msg = random.choice( list( self.messages ) )
            self.messages.remove( msg )
        else:
            msg = None
        return msg
        
        

class InstanceRecord( object ):
    """This is a bookkeeping class, which keeps a record of all proposals we've seen or undertaken for a given record,
    both on the acceptor and the leader"""
    def __init__( self ):
        self.protocols = {}
        self.highestID = (-1,-1)
        self.value = None
    def addProtocol( self, protocol ):
        self.protocols[ protocol.proposalID ] = protocol
        if protocol.proposalID[1] > self.highestID[1] or (protocol.proposalID[1] == self.highestID[1] and protocol.proposalID[0] > self.highestID[0]):
            self.highestID = protocol.proposalID
    def getProtocol( self, protocolID ):
        return self.protocols[ protocolID ]

    def cleanProtocols( self ):
        keys = self.protocols.keys( )
        for k in keys:
            protocol = self.protocols[k]
            if protocol.state == PaxosLeaderProtocol.STATE_ACCEPTED:
                print "Deleting protocol"
                del self.protocols[k]
        
class PaxosLeader( object ):
    def __init__(self, port, leaders=None, acceptors=None):
        self.port = port
        if leaders == None:
            self.leaders = []
        else:
            self.leaders = leaders
        if acceptors == None:
            self.acceptors = []
        else:
            self.acceptors = acceptors
        self.group = self.leaders + self.acceptors        
        self.isPrimary = False
        self.proposalCount = 0
        self.msgPump = MessagePump( self, port )
        self.instances = {}
        self.hbListener = PaxosLeader.HeartbeatListener( self )
        self.hbSender = PaxosLeader.HeartbeatSender( self )
        self.highestInstance = -1
        self.stopped = True
        # The last time we tried to fix up any gaps
        self.lasttime = time.time( )

    #------------------------------------------------------
    # These two classes listen for heartbeats from other leaders
    # and, if none appear, tell this leader that it should
    # be the primary
        
    class HeartbeatListener( threading.Thread ):
        def __init__( self, leader ):
            self.leader = leader
            self.queue = Queue.Queue( )
            self.abort = False
            threading.Thread.__init__( self )

        def newHB( self, message ):
            self.queue.put( message )

        def doAbort( self ): self.abort = True
            
        def run( self ):
            elapsed = 0
            while not self.abort:
                s = time.time( )
                try:
                    hb = self.queue.get( True, 2 )
                    # Easy way to settle conflicts - if your port number is bigger than mine,
                    # you get to be the leader
                    if hb.source > self.leader.port:
                        self.leader.setPrimary( False )
                except: # Nothing was got
                    self.leader.setPrimary( True )

    class HeartbeatSender( threading.Thread ):
        def __init__( self, leader ):
            self.leader = leader
            self.abort = False
            threading.Thread.__init__( self )            

        def doAbort( self ): self.abort = True
            
        def run( self ):
            while not self.abort:
                time.sleep( 1 )
                if self.leader.isPrimary:
                    msg = Message( Message.MSG_HEARTBEAT )
                    msg.source = self.leader.port
                    for l in self.leader.leaders:
                        msg.to = l
                        self.leader.sendMessage( msg )

    #------------------------------------------------------
    def sendMessage( self, message ):
        self.msgPump.sendMessage( message )
                
    def start( self ):
        self.hbSender.start( )
        self.hbListener.start( )
        self.msgPump.start( )
        self.stopped = False

    def stop( self ):
        self.hbSender.doAbort( )
        self.hbListener.doAbort( )
        self.msgPump.doAbort( )
        self.stopped = True

    def setPrimary( self, primary ):
        if self.isPrimary != primary:
            # Only print if something's changed
            if primary:
                print "I (%s) am the leader" % self.port
            else:
                print "I (%s) am NOT the leader" % self.port            
        self.isPrimary = primary

    #------------------------------------------------------        

    def getGroup( self ):
        return self.group

    def getLeaders( self ):
        return self.leaders

    def getAcceptors( self ):
        return self.acceptors

    def getQuorumSize( self ):
        return (len(self.getAcceptors( ) ) / 2) + 1

    def getInstanceValue( self, instanceID ):
        if instanceID in self.instances:
            return self.instances[ instanceID ].value
        return None
    
    def getHistory( self ):
        return [ self.getInstanceValue( i ) for i in xrange( 1, self.highestInstance+1 ) ]

    def getNumAccepted( self ):
        return len( [v for v in self.getHistory( ) if v != None] )
    
    #------------------------------------------------------    

    def findAndFillGaps( self ):
        # if no message is received, we take the chance to do a little cleanup
        for i in xrange(1,self.highestInstance):
            if self.getInstanceValue( i ) == None:
                print "Filling in gap", i
                self.newProposal( 0, i ) # This will either eventually commit an already accepted value, or fill in the gap with 0 or no-op
        self.lasttime = time.time( )

    def garbageCollect( self ):
        for i in self.instances:
            self.instances[i].cleanProtocols( )

    def recvMessage( self, message ):
        """Message pump will call this periodically, even if there's no message available"""
        if self.stopped: return
        if message == None:
            # Only run every 15s otherwise you run the risk of cutting good protocols off in their prime :(            
            if self.isPrimary and time.time( ) - self.lasttime > 15.0: 
                self.findAndFillGaps( )
                self.garbageCollect( )
            return
        if message.command == Message.MSG_HEARTBEAT:
            self.hbListener.newHB( message )
            return True
        if message.command == Message.MSG_EXT_PROPOSE:
            print "External proposal received at", self.port, self.highestInstance
            if self.isPrimary:
                self.newProposal( message.value )
            # else ignore - we're getting  proposals when we're not the primary
            # what we should do, if we were being kind, is reply with a message saying 'leader has changed'
            # and giving the address of the new one. However, we might just as well have failed.
            return True
        if self.isPrimary and message.command != Message.MSG_ACCEPTOR_ACCEPT:
            self.instances[ message.instanceID ].getProtocol(message.proposalID).doTransition( message )        
        # It's possible that, while we still think we're the primary, we'll get a
        # accept message that we're only listening in on. 
        # We are interested in hearing all accepts, so we play along by pretending we've got the protocol
        # that's getting accepted and listening for a quorum as per usual
        if message.command == Message.MSG_ACCEPTOR_ACCEPT:
            if message.instanceID not in self.instances:
                self.instances[ message.instanceID ] = InstanceRecord( )
            record = self.instances[ message.instanceID ]
            if message.proposalID not in record.protocols:
                protocol = PaxosLeaderProtocol( self )
                # We just massage this protocol to be in the waiting-for-accept state
                protocol.state = PaxosLeaderProtocol.STATE_AGREED
                protocol.proposalID = message.proposalID
                protocol.instanceID = message.instanceID
                protocol.value = message.value
                record.addProtocol( protocol )
            else:
                protocol = record.getProtocol( message.proposalID )
            # Should just fall through to here if we initiated this protocol instance
            protocol.doTransition( message )
        return True

    def newProposal( self, value, instance = None ):
        protocol = PaxosLeaderProtocol( self )
        if instance == None:
            self.highestInstance += 1
            instanceID = self.highestInstance
        else:
            instanceID = instance
        self.proposalCount += 1
        id = (self.port, self.proposalCount )
        if instanceID in self.instances:
            record = self.instances[ instanceID ]
        else:
            record = InstanceRecord( )
            self.instances[ instanceID ] = record
        protocol.propose( value, id, instanceID )
        record.addProtocol( protocol )        
            
    def notifyLeader( self, protocol, message ):
        # Protocols call this when they're done
        if protocol.state == PaxosLeaderProtocol.STATE_ACCEPTED:
           print "Protocol instance %s accepted with value %s" % (message.instanceID, message.value)
           self.instances[ message.instanceID ].accepted = True
           self.instances[ message.instanceID ].value = message.value
           self.highestInstance = max( message.instanceID, self.highestInstance )
           return
        if protocol.state == PaxosLeaderProtocol.STATE_REJECTED:
            # Look at the message to find the value, and then retry
            # Eventually, assuming that the acceptors will accept some value for
            # this instance, the protocol will complete.
            self.proposalCount = max( self.proposalCount, message.highestPID[1] )
            self.newProposal( message.value )
            return True
        if protocol.state == PaxosLeaderProtocol.STATE_UNACCEPTED:
            pass            
        
class PaxosLeaderProtocol( object ):
    # State variables
    STATE_UNDEFINED = -1
    STATE_PROPOSED = 0
    STATE_AGREED = 1
    STATE_REJECTED = 2
    STATE_ACCEPTED = 3
    STATE_UNACCEPTED = 4
    
    def __init__( self, leader ):
        self.leader = leader
        self.state = PaxosLeaderProtocol.STATE_UNDEFINED
        self.proposalID = (-1,-1)
        self.agreecount, self.acceptcount = (0,0)
        self.rejectcount, self.unacceptcount = (0,0)
        self.instanceID = -1
        self.highestseen = (0,0)

    def propose( self, value, pID, instanceID ):
        self.proposalID = pID
        self.value = value
        self.instanceID = instanceID
        message = Message( Message.MSG_PROPOSE )
        message.proposalID = pID
        message.instanceID = instanceID
        message.value = value 
        for server in self.leader.getAcceptors( ):
            message.to = server
            self.leader.sendMessage( message )
        self.state = PaxosLeaderProtocol.STATE_PROPOSED
        return self.proposalID
        
    def doTransition( self, message ):
        """We run the protocol like a simple state machine. It's not always
        okay to error on unexpected inputs, however, due to message delays, so we silently
        ignore inputs that we're not expecting."""
        if self.state == PaxosLeaderProtocol.STATE_PROPOSED:
            if message.command == Message.MSG_ACCEPTOR_AGREE:
                self.agreecount += 1
                if self.agreecount >= self.leader.getQuorumSize( ):
#                    print "Achieved agreement quorum, last value replied was:", message.value
                    if message.value != None: # If it's none, can do what we like. Otherwise we have to take the highest seen proposal
                        if message.sequence[0] > self.highestseen[0] or (message.sequence[0] == self.highestseen[0] and message.sequence[1] > self.highestseen[1]):
                            self.value = message.value
                            self.highestseen = message.sequence
                    self.state = PaxosLeaderProtocol.STATE_AGREED
                    # Send 'accept' message to group                    
                    msg = Message( Message.MSG_ACCEPT )
                    msg.copyAsReply( message )
                    msg.value = self.value
                    msg.leaderID = msg.to
                    for s in self.leader.getAcceptors( ):
                        msg.to = s
                        self.leader.sendMessage( msg )
                    self.leader.notifyLeader( self, message )
                return True
            if message.command == Message.MSG_ACCEPTOR_REJECT:
                self.rejectcount += 1
                if self.rejectcount >= self.leader.getQuorumSize( ):
                    self.state = PaxosLeaderProtocol.STATE_REJECTED                    
                    self.leader.notifyLeader( self, message )
                return True
        if self.state == PaxosLeaderProtocol.STATE_AGREED:
            if message.command == Message.MSG_ACCEPTOR_ACCEPT:
                self.acceptcount += 1
                if self.acceptcount >= self.leader.getQuorumSize( ):
                    self.state = PaxosLeaderProtocol.STATE_ACCEPTED
                    self.leader.notifyLeader( self, message )
            if message.command == Message.MSG_ACCEPTOR_UNACCEPT:
                self.unacceptcount += 1
                if self.unacceptcount >= self.leader.getQuorumSize( ):
                    self.state = PaxosLeaderProtocol.STATE_UNACCEPTED
                    self.leader.notifyLeader( self, message )
        pass    
    
class PaxosAcceptor( object ):            
    def __init__(self, port,leaders ):
        self.port = port
        self.leaders = leaders
        self.instances = {}
        self.msgPump = MessagePump( self, self.port )
        self.failed = False

    def start( self ):
        self.msgPump.start( )

    def stop( self ):
        self.msgPump.doAbort( )

    def fail( self ):
        self.failed = True
    def recover( self ):
        self.failed = False

    def sendMessage( self, message ):
        self.msgPump.sendMessage( message )
        
    def recvMessage( self, message ):
        if message == None: return
        if self.failed:
            return # Failure means ignored and lost messages
        if message.command == Message.MSG_PROPOSE: 
            if message.instanceID not in self.instances:
                record = InstanceRecord( )                
                self.instances[ message.instanceID ] = record
            protocol = PaxosAcceptorProtocol( self )
            protocol.recvProposal( message )
            self.instances[ message.instanceID ].addProtocol( protocol )
        else:
            self.instances[ message.instanceID ].getProtocol( message.proposalID ).doTransition( message )

    def notifyClient( self, protocol, message ):
        if protocol.state == PaxosAcceptorProtocol.STATE_PROPOSAL_ACCEPTED:
            self.instances[ protocol.instanceID ].value = message.value
#            print "Proposal accepted at client: ", message.value


    def getHighestAgreedProposal( self, instance ):
        return self.instances[ instance ].highestID

    def getInstanceValue( self, instance ):
        return self.instances[ instance ].value
            
    
class PaxosAcceptorProtocol( object ):
    # State variables
    STATE_UNDEFINED = -1
    STATE_PROPOSAL_RECEIVED = 0
    STATE_PROPOSAL_REJECTED = 1
    STATE_PROPOSAL_AGREED = 2
    STATE_PROPOSAL_ACCEPTED = 3
    STATE_PROPOSAL_UNACCEPTED = 4

    def __init__( self, client ):
        self.client = client
        self.state = PaxosAcceptorProtocol.STATE_UNDEFINED

    def recvProposal( self, message ):
        if message.command == Message.MSG_PROPOSE:
            self.proposalID = message.proposalID
            self.instanceID = message.instanceID
            # What's the highest already agreed proposal for this instance?
            (port, count) = self.client.getHighestAgreedProposal( message.instanceID )
            # Check if this proposal is numbered higher
            if count < self.proposalID[0] or (count == self.proposalID[0] and port < self.proposalID[1]):
                # Send agreed message back, with highest accepted value (if it exists)
                self.state = PaxosAcceptorProtocol.STATE_PROPOSAL_AGREED
#                print "Agreeing to proposal: ", message.instanceID, message.value
                value = self.client.getInstanceValue( message.instanceID )
                msg = Message( Message.MSG_ACCEPTOR_AGREE )
                msg.copyAsReply( message )
                msg.value = value
                msg.sequence = (port, count)
                self.client.sendMessage( msg )
            else:
                # Too late, we already told someone else we'd do it
                # Send reject message, along with highest proposal id and its value
                self.state = PaxosAcceptorProtocol.STATE_PROPOSAL_REJECTED
            return self.proposalID
        else:
            # error, trying to receive a non-proposal?
            pass


    def doTransition( self, message ):
        if self.state == PaxosAcceptorProtocol.STATE_PROPOSAL_AGREED and message.command == Message.MSG_ACCEPT:
            self.state = PaxosAcceptorProtocol.STATE_PROPOSAL_ACCEPTED
            # Could check on the value here, if we don't trust leaders to honour what we tell them
            # send reply to leader acknowledging
            msg = Message( Message.MSG_ACCEPTOR_ACCEPT )
            msg.copyAsReply( message )
            for l in self.client.leaders:
                msg.to = l
                self.client.sendMessage( msg )
            self.notifyClient( message )            
            return True
        
        raise Exception( "Unexpected state / command combination!" )

    def notifyClient( self, message ):
        self.client.notifyClient( self, message )

import time
if __name__ == '__main__':
    numclients = 5
    clients = [ PaxosAcceptor( port, [54321,54322] ) for port in xrange( 64320, 64320+numclients ) ]
    leader = PaxosLeader( 54321, [54322], [c.port for c in clients] )
    leader2 = PaxosLeader( 54322, [54321], [c.port for c in clients] )
    leader.start( )
    leader.setPrimary( True )
    leader2.setPrimary( True )
    leader2.start( )
    for c in clients:
        c.start( )

    clients[0].fail( )
    clients[1].fail( )
#    clients[2].fail( )
    
    # Send some proposals through to test
    s = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    start = time.time( )
    for i in xrange(1000):
        m = Message( Message.MSG_EXT_PROPOSE )
        m.value = 0 + i
        m.to = 54322
        bytes = pickle.dumps( m )
        s.sendto( bytes, ("localhost", m.to) )

    while leader2.getNumAccepted( ) < 999:
        print "Sleeping for 1s -- accepted:", leader2.getNumAccepted( )
        time.sleep( 1 )
    end = time.time( )    
        
    print "Sleeping for 10s"
    time.sleep( 10 )
    print "Stopping leaders"
    leader.stop( )    
    leader2.stop( )
    print "Stopping clients"
    for c in clients:
        c.stop( )

    print "Leader 1 history: ", leader.getHistory( )
    print "Leader 2 history: ", leader2.getHistory( )
    print end - start
