package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;

import java.io.Serializable;
import java.util.List;

public class L1Cache extends Cache implements Coordinator {

    private final ACCoordinator acCoordinator = new ACCoordinator(this);

    public L1Cache(String id) {
        super(id);
    }

    static public Props props(String id) {
        return Props.create(Cache.class, () -> new L1Cache(id));
    }

    @Override
    protected void forwardMessageToNext(Serializable message, MessageType messageType) {
        long messageDelay = 0;
        if (message instanceof Message) {
            Message msg = (Message) message;
            if (msg.isMessageDelayedAtL1()) {
                messageDelay = msg.getL1MessageDelay();
            }
        }

        this.send(message, this.database, messageDelay);
    }

    @Override
    protected void handleRefillMessage(RefillMessage message) {
        // just multicast to all L2s
        Logger.refill(this.id, LoggerOperationType.MULTICAST, message.getKey(), message.getValue(), 0,
                message.getUpdateCount(), 0, false, false, true);
        this.multicast(message, this.l2Caches);
        this.abortWrite(message.getKey());
    }

    @Override
    protected void handleTimeoutMessage(TimeoutMessage message) {
        if (message.getType() == MessageType.CRITICAL_WRITE_REQUEST) {
            CritWriteRequestMessage requestMessage = (CritWriteRequestMessage) message.getMessage();
            int key = requestMessage.getKey();

            if (this.isWriteUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                // reset and just timeout
                this.abortCritWrite(key, true);
            }
        } else if (message.getType() == MessageType.WRITE) {
            WriteMessage writeMessage = (WriteMessage) message.getMessage();
            int key = writeMessage.getKey();

            if (this.isWriteUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                // reset and timeout
                this.abortWrite(key);
            }
        } else if (message.getType() == MessageType.CRITICAL_WRITE) {
            CritWriteMessage critWriteMessage = (CritWriteMessage) message.getMessage();
            int key = critWriteMessage.getKey();

            if (this.isWriteUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                this.abortCritWrite(key, false);
            }
        } else if (message.getType() == MessageType.READ) {
            ReadMessage readMessage = (ReadMessage) message.getMessage();
            int key = readMessage.getKey();

            if (this.isReadUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                this.removeUnconfirmedRead(key);
            }
        }
    }

    @Override
    protected void handleCritWriteRequestMessage(CritWriteRequestMessage message, boolean isOk) {
        int key = message.getKey();

        if (isOk) {
            // first lock
            this.lockKey(key);
            // iff everything is ok, then multicast the request to all L2s, otherwise force a timeout
            Logger.criticalWriteRequest(this.id, LoggerOperationType.MULTICAST, key, true);
            this.acCoordinator.setCritWriteConfig(key);
            this.multicast(message, this.l2Caches);
            this.setMulticastTimeout(message, MessageType.CRITICAL_WRITE_REQUEST);
            // set as unconfirmed with no sender if not already srt as unconfirmed
            this.addUnconfirmedWrite(key, ActorRef.noSender());
        } else {
            this.abortCritWrite(key, true);
        }
    }

    @Override
    protected void handleCritWriteVoteMessage(CritWriteVoteMessage message) {
        int key = message.getKey();
        boolean isOk = message.isOk();
        Logger.criticalWriteVote(this.id, LoggerOperationType.RECEIVED, key, isOk);

        this.acCoordinator.onCritWriteVoteMessage(message);
    }

    @Override
    protected void handleCritWriteAbortMessage(CritWriteAbortMessage message) {
        int key = message.getKey();
        // reset/abort
        this.abortCritWrite(key, true);
    }

    @Override
    protected void handleCritWriteCommitMessage(CritWriteCommitMessage message) {
        // reset critical write
        this.abortCritWrite(message.getKey(), false);
        // multicast commit to all L2s
        Logger.criticalWriteCommit(this.id, LoggerOperationType.MULTICAST, message.getKey(), message.getValue(), 0,
                message.getUpdateCount(), 0);
        this.multicast(message, this.l2Caches);
    }

    @Override
    protected void handleErrorMessage(ErrorMessage message) {
        MessageType messageType = message.getMessageType();
        int key = message.getKey();

        if (messageType == MessageType.WRITE && this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, LoggerOperationType.SEND, messageType, key, false, "Forward error message");
            // tell L2 about message
            ActorRef l2Cache = this.getUnconfirmedActorForWrit(key);
            this.send(message, l2Cache);
            // reset
            this.abortWrite(key);
        } else if (messageType == MessageType.CRITICAL_WRITE && this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, LoggerOperationType.SEND, messageType, key, false, "Forward error message");
            // tell L2 about message
            ActorRef l2Cache = this.getUnconfirmedActorForWrit(key);
            this.send(message, l2Cache);
            // reset and just timeout
            this.abortCritWrite(key, false);
        } else if ((messageType == MessageType.READ || messageType == MessageType.CRITICAL_READ) && this.isReadUnconfirmed(key)) {
            if (messageType == MessageType.READ) {
                Logger.error(this.id, LoggerOperationType.MULTICAST, messageType, key, false, "Forward error message");
            } else {
                Logger.error(this.id, LoggerOperationType.MULTICAST, messageType, key, false, "Forward error message");
            }

            // tell L2 about message
            List<ActorRef> l2Caches = this.getUnconfirmedActorsForRead(key);
            this.multicast(message, l2Caches);
            // reset
            this.removeUnconfirmedRead(key);
        }
    }

    @Override
    protected boolean isCritWriteOk(int key) {
        if (this.isWriteUnconfirmed(key)) {
            /*
            This is the L1 contacted by the L2. The write is already unconfirmed.
            Therefore, only need to check if key is locked.
             */
            return !this.isKeyLocked(key);
        } else {
            /*
            This L1 has not been contacted by the initiating L2. Therefore, the write can't
            be unconfirmed by another request and must be unlocked.
             */
            return !this.isKeyLocked(key) && !this.isWriteUnconfirmed(key);
        }
    }

    @Override
    protected boolean isL1Cache() {
        return true;
    }

    @Override
    protected void sendWriteConfirm(int key, int value, int updateCount) {
        // Do nothing
    }

    @Override
    protected void handleFill(int key) {
        if (this.isReadUnconfirmed(key)) {
            int value = this.getValueOrElse(key);
            int updateCount = this.getUpdateCountOrElse(key);

            // multicast to L2s who have requested the key
            List<ActorRef> requestedL2s = this.getUnconfirmedActorsForRead(key);
            FillMessage fillMessage = new FillMessage(key, value, updateCount);
            Logger.fill(this.id, LoggerOperationType.MULTICAST, key, value, 0, updateCount, 0);
            this.multicast(fillMessage, requestedL2s);
            // afterwards reset for key
            this.removeUnconfirmedRead(key);
        }
    }

    @Override
    protected void flush() {
        super.flush();
        this.acCoordinator.resetCritWriteConfig();
    }

    @Override
    protected void recover() {
        super.recover();
        // send flush to all L2s
        FlushMessage flushMessage = new FlushMessage(this.getSelf());
        Logger.flush(this.id, LoggerOperationType.MULTICAST);
        this.multicast(flushMessage, this.l2Caches);
    }

    @Override
    public boolean haveAllParticipantsVoted(int voteCount) {
        return voteCount == this.l2Caches.size();
    }

    @Override
    public void onVoteOk(int key, int value) {
        // set as unconfirmed with no sender, just to block all new write requests
        this.addUnconfirmedWrite(key, ActorRef.noSender());

        CritWriteVoteMessage critWriteVoteMessage = new CritWriteVoteMessage(key, true);
        Logger.criticalWriteVote(this.id, LoggerOperationType.SEND, key, true);
        this.send(critWriteVoteMessage, this.database);
    }

    @Override
    public void abortCritWrite(int key, boolean multicastAbort) {
        this.abortWrite(key);
        this.acCoordinator.resetCritWriteConfig();

        // answer abort
        CritWriteVoteMessage voteMessage = new CritWriteVoteMessage(key, false);
        Logger.criticalWriteVote(this.id, LoggerOperationType.SEND, key, false);
        this.send(voteMessage, this.database);

        // multicast abort message to L2s
        if (multicastAbort) {
            Logger.criticalWriteAbort(this.id, LoggerOperationType.MULTICAST, key);
            CritWriteAbortMessage abortMessage = new CritWriteAbortMessage(key);
            this.multicast(abortMessage, this.l2Caches);
        }
    }
}
