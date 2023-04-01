package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerType;

import java.io.Serializable;
import java.util.List;

public class L1Cache extends Cache implements Coordinator {

    private final ACCoordinator acCoordinator = new ACCoordinator(this);
    private boolean isCritWriteRequestConfirmed = false;

    public L1Cache(String id) {
        super(id);
    }

    static public Props props(String id) {
        return Props.create(Cache.class, () -> new L1Cache(id));
    }

    private void resetCritWriteConfig(int key) {
        this.resetWriteConfig(key);
        this.acCoordinator.resetCritWriteConfig();
    }

    @Override
    protected void forwardMessageToNext(Serializable message, TimeoutType timeoutType) {
        this.database.tell(message, this.getSelf());
    }

    @Override
    protected void handleWriteMessage(WriteMessage message) {
        // what todo here, also L2
    }

    @Override
    protected void handleRefillMessage(RefillMessage message) {
        // just multicast to all L2s
        Logger.refill(this.id, LoggerOperationType.MULTICAST, message.getKey(), message.getValue(), 0,
                message.getUpdateCount(), 0, false, false, true);
        this.multicast(message, this.l2Caches);
        this.resetWriteConfig(message.getKey());
    }

    @Override
    protected void handleTimeoutMessage(TimeoutMessage message) {
        if (message.getType() == TimeoutType.CRIT_WRITE_REQUEST) {
            CritWriteRequestMessage requestMessage = (CritWriteRequestMessage) message.getMessage();
            int key = requestMessage.getKey();

            if (!this.isCritWriteRequestConfirmed) {
                Logger.timeout(this.id, message.getType());
                // reset and just timeout
                this.resetCritWriteConfig(key);
                this.isCritWriteRequestConfirmed = false;
            }
        } else if (message.getType() == TimeoutType.WRITE) {
            WriteMessage writeMessage = (WriteMessage) message.getMessage();
            int key = writeMessage.getKey();

            if (this.isWriteUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                // reset and timeout
                this.resetWriteConfig(key);
            }
        } else if (message.getType() == TimeoutType.READ) {
            ReadMessage readMessage = (ReadMessage) message.getMessage();
            int key = readMessage.getKey();

            if (this.isReadUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                this.resetReadConfig(key);
            }
        }
    }

    @Override
    protected void handleCritWriteMessage(CritWriteMessage message) {
        Logger.criticalWrite(this.id, LoggerOperationType.SEND, message.getKey(), message.getValue(), false);
        this.forwardMessageToNext(message, TimeoutType.CRIT_WRITE);
    }

    @Override
    protected void handleCritWriteRequestMessage(CritWriteRequestMessage message, boolean isOk) {
        if (isOk) {
            // iff everything is ok, then multicast the request to all L2s, otherwise force a timeout
            Logger.criticalWriteRequest(this.id, LoggerOperationType.MULTICAST, message.getKey(), true);
            this.acCoordinator.setCritWriteConfig(message.getKey());
            this.multicast(message, this.l2Caches);
            this.isCritWriteRequestConfirmed = false;
            this.setMulticastTimeout(message, TimeoutType.CRIT_WRITE_REQUEST);
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
        this.resetCritWriteConfig(key);
        // multicast abort to L2s
        Logger.criticalWriteAbort(this.id, LoggerOperationType.MULTICAST, key);
        this.multicast(message, this.l2Caches);
    }

    @Override
    protected void handleCritWriteCommitMessage(CritWriteCommitMessage message) {
        // reset critical write
        this.resetCritWriteConfig(message.getKey());
        // multicast commit to all L2s
        Logger.criticalWriteCommit(this.id, LoggerOperationType.MULTICAST, message.getKey(), message.getValue(), 0,
                message.getUpdateCount(), 0);
        this.multicast(message, this.l2Caches);
    }

    @Override
    protected void handleErrorMessage(ErrorMessage message) {
        TimeoutType messageType = message.getMessageType();
        int key = message.getKey();

        if (messageType == TimeoutType.WRITE && this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, LoggerType.WRITE, key, false, "Received error message");
            // tell L2 about message
            ActorRef l2Cache = this.unconfirmedWrites.get(key);
            l2Cache.tell(message, this.getSelf());
            // reset
            this.resetWriteConfig(key);
        } else if (messageType == TimeoutType.CRIT_WRITE && !this.isCritWriteRequestConfirmed) {
            Logger.error(this.id, LoggerType.CRITICAL_WRITE, key, false, "Received error message");
            // tell L2 about message
            ActorRef l2Cache = this.unconfirmedWrites.get(key);
            l2Cache.tell(message, this.getSelf());
            // reset and just timeout
            this.resetCritWriteConfig(key);
            this.isCritWriteRequestConfirmed = false;
        } else if ((messageType == TimeoutType.READ || messageType == TimeoutType.CRIT_READ) && this.isReadUnconfirmed(key)) {
            if (messageType == TimeoutType.READ) {
                Logger.error(this.id, LoggerType.READ, key, false, "Received error message");
            } else {
                Logger.error(this.id, LoggerType.CRITICAL_READ, key, false, "Received error message");
            }

            // tell L2 about message
            List<ActorRef> l2Caches = this.unconfirmedReads.get(key);
            this.multicast(message, l2Caches);
            // reset
            this.resetReadConfig(key);
        }
    }

    @Override
    protected boolean isCritWriteOk(int key) {
        return !this.data.isLocked(key) && !this.isWriteUnconfirmed(key);
    }

    @Override
    protected boolean isL1Cache() {
        return true;
    }

    /*@Override
    protected void multicastReFillMessageIfNeeded(int key, int value, int updateCount, ActorRef sender) {
        RefillMessage reFillMessage = new RefillMessage(key, value, updateCount);
        Logger.refill(this.id, LoggerOperationType.MULTICAST, key, value, 0, updateCount, 0, false, false, true);

        // todo heck this, why???
        /*if (sender != ActorRef.noSender()) {
            this.multicast(reFillMessage, this.l2Caches);
        }*/
        /*for (ActorRef cache: this.l2Caches) {
            if (cache != sender) {
                cache.tell(reFillMessage, this.getSelf());
            }
        }
    }*/

    @Override
    protected void handleFill(int key) {
        if (this.isReadUnconfirmed(key)) {
            int value = this.data.getValueForKey(key).get();
            int updateCount = this.data.getUpdateCountForKey(key).get();

            // multicast to L2s who have requested the key
            List<ActorRef> requestedL2s = this.unconfirmedReads.get(key);
            FillMessage fillMessage = new FillMessage(key, value, updateCount);
            Logger.fill(this.id, LoggerOperationType.MULTICAST, key, value, 0, updateCount, 0);
            this.multicast(fillMessage, requestedL2s);
            // afterwards reset for key
            this.resetReadConfig(key);
        }
    }

    @Override
    protected void flush() {
        super.flush();
        this.isCritWriteRequestConfirmed = false;
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
        // crit write request is now confirmed
        this.isCritWriteRequestConfirmed = true;
        // got OK vote from all L2s, lock and answer back to DB
        this.data.lockValueForKey(key);
        // set as unconfirmed with no sender, just to block all new write requests
        this.addUnconfirmedWrite(key, ActorRef.noSender());

        CritWriteVoteMessage critWriteVoteMessage = new CritWriteVoteMessage(key, true);
        Logger.criticalWriteVote(this.id, LoggerOperationType.SEND, key, true);
        this.database.tell(critWriteVoteMessage, this.getSelf());
    }

    @Override
    public void abortCritWrite(int key) {
        this.resetCritWriteConfig(key);
    }
}
