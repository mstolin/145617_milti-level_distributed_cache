package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger;

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
        this.multicast(message, this.l2Caches);
        this.resetWriteConfig(message.getKey());
    }

    @Override
    protected void handleTimeoutMessage(TimeoutMessage message) {
        if (message.getType() == TimeoutType.CRIT_WRITE_REQUEST) {
            CritWriteRequestMessage requestMessage = (CritWriteRequestMessage) message.getMessage();
            int key = requestMessage.getKey();

            if (this.isWriteUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                // reset and just timeout
                this.resetCritWriteConfig(key);
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
        this.acCoordinator.setCritWriteConfig(message.getValue());
    }

    @Override
    protected void handleCritWriteRequestMessage(CritWriteRequestMessage message, boolean isOk) {
        if (isOk) {
            // iff everything is ok, then multicast the request to all L2s
            this.multicast(message, this.l2Caches);
            this.setMulticastTimeout(message, TimeoutType.CRIT_WRITE_REQUEST);
        }
    }

    @Override
    protected void handleCritWriteVoteMessage(CritWriteVoteMessage message) {
        this.acCoordinator.onCritWriteVoteMessage(message, this.id);
    }

    @Override
    protected void handleCritWriteAbortMessage(CritWriteAbortMessage message) {
        int key = message.getKey();
        // reset/abort
        this.resetCritWriteConfig(key);
        // multicast abort to L2s
        this.multicast(message, this.l2Caches);
    }

    @Override
    protected void handleCritWriteCommitMessage(CritWriteCommitMessage message) {
        // reset critical write
        this.resetCritWriteConfig(message.getKey());
        // multicast commit to all L2s
        this.multicast(message, this.l2Caches);
    }

    @Override
    protected void multicastReFillMessageIfNeeded(int key, int value, int updateCount, ActorRef sender) {
        RefillMessage reFillMessage = new RefillMessage(key, value, updateCount);

        if (sender != ActorRef.noSender()) {
            this.multicast(reFillMessage, this.l2Caches);
        }
        for (ActorRef cache: this.l2Caches) {
            if (cache != sender) {
                cache.tell(reFillMessage, this.getSelf());
            }
        }
    }

    @Override
    protected void handleFill(int key) {
        if (this.isReadUnconfirmed(key)) {
            int value = this.data.getValueForKey(key).get();
            int updateCount = this.data.getUpdateCountForKey(key).get();

            // multicast to L2s who have requested the key
            List<ActorRef> requestedL2s = this.unconfirmedReads.get(key);
            FillMessage fillMessage = new FillMessage(key, value, updateCount);
            this.multicast(fillMessage, requestedL2s);
            // afterwards reset for key
            this.resetReadConfig(key);
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
        System.out.printf("%s - Flush all L2s\n", this.id);
        FlushMessage flushMessage = new FlushMessage(this.getSelf());
        this.multicast(flushMessage, this.l2Caches);
    }

    @Override
    public boolean haveAllParticipantsVoted(int voteCount) {
        return voteCount == this.l2Caches.size();
    }

    @Override
    public void onVoteOk(int key, int value) {
        // got OK vote from all L2s, lock and answer back to DB
        this.data.lockValueForKey(key);
        CritWriteVoteMessage critWriteVoteMessage = new CritWriteVoteMessage(key, true);
        this.database.tell(critWriteVoteMessage, this.getSelf());
        // reset
        this.resetCritWriteConfig(key);
    }

    @Override
    public void abortCritWrite(int key) {
        this.resetCritWriteConfig(key);
    }
}
