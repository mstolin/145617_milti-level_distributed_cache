package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;

import java.io.Serializable;
import java.util.List;

public class L2Cache extends Cache {

    /** Is this the L2 requested by the client for critical write? */
    private boolean isPrimaryL2ForCritWrite = false;

    public L2Cache(String id) {
        super(id);
    }

    static public Props props(String id) {
        return Props.create(Cache.class, () -> new L2Cache(id));
    }

    @Override
    protected void forwardMessageToNext(Serializable message, TimeoutType timeoutType) {
        this.mainL1Cache.tell(message, this.getSelf());
        this.setTimeout(message, this.mainL1Cache, timeoutType);
    }

    @Override
    protected void handleWriteMessage(WriteMessage message) {
        // todo what todo here also l1
    }

    @Override
    protected void handleRefillMessage(RefillMessage message) {
        int key = message.getKey();
        if (this.isWriteUnconfirmed(key)) {
            // tell client write confirm
            ActorRef client = this.unconfirmedWrites.get(key);
            int value = message.getValue();
            int updateCount = message.getUpdateCount();
            WriteConfirmMessage confirmMessage = new WriteConfirmMessage(key, value, updateCount);
            Logger.writeConfirm(this.id, LoggerOperationType.SEND, key, value, 0, updateCount, 0);
            client.tell(confirmMessage, this.getSelf());
            // reset timeout
            this.resetWriteConfig(key);
        }
    }

    @Override
    protected void handleTimeoutMessage(TimeoutMessage message) {
        // forward message to DB, no need for timeout since DB can't timeout
        /*
        TODO Was ist wenn DB timeout wegen ein lock?
         */
        if (message.getType() == TimeoutType.READ) {
            ReadMessage readMessage = (ReadMessage) message.getMessage();
            int key = readMessage.getKey();

            // if the key is in this map, then no ReadReply has been received for the key
            if (this.isReadUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                Logger.read(this.id, LoggerOperationType.SEND, key, readMessage.getUpdateCount(),
                        this.data.getUpdateCountForKey(key).orElse(0), this.data.isLocked(key), false, true);
                this.database.tell(readMessage, this.getSelf());
            }
        } else if (message.getType() == TimeoutType.CRIT_READ) {
            CritReadMessage critReadMessage = (CritReadMessage) message.getMessage();
            int key = critReadMessage.getKey();

            // if the key is in this map, then no ReadReply has been received for the key
            if (this.isReadUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                this.database.tell(critReadMessage, this.getSelf());
            }
        } else if (message.getType() == TimeoutType.WRITE) {
            WriteMessage writeMessage = (WriteMessage) message.getMessage();
            int key = writeMessage.getKey();

            if (this.isWriteUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                Logger.write(this.id, LoggerOperationType.SEND, key, writeMessage.getValue(), this.data.isLocked(key));
                this.database.tell(writeMessage, this.getSelf());
            }
        } else if (message.getType() == TimeoutType.CRIT_WRITE) {
            CritWriteMessage writeMessage = (CritWriteMessage) message.getMessage();
            int key = writeMessage.getKey();

            if (this.isWriteUnconfirmed(key)) {
                // do not forward to DB when crit write fails
                Logger.timeout(this.id, message.getType());
                this.resetWriteConfig(key);
                this.isPrimaryL2ForCritWrite = false;
            }
        }
    }

    @Override
    protected void handleCritWriteMessage(CritWriteMessage message) {
        Logger.criticalWrite(this.id, LoggerOperationType.SEND, message.getKey(), message.getValue(), false);
        // set as unconfirmed
        this.addUnconfirmedWrite(message.getKey(),this.getSender());
        this.isPrimaryL2ForCritWrite = true;
        // forward to L1
        this.forwardMessageToNext(message, TimeoutType.CRIT_WRITE);
    }

    @Override
    protected void handleCritWriteRequestMessage(CritWriteRequestMessage message, boolean isOk) {
        int key = message.getKey();
        if (isOk) {
            // just lock data
            this.data.lockValueForKey(key);

            if (!this.isPrimaryL2ForCritWrite) {
                // is this is not the L2 contacted by the client, add write as unconfirmed with no sender
                this.addUnconfirmedWrite(key, ActorRef.noSender());
            }
        }
        // answer back
        CritWriteVoteMessage critWriteVoteOkMessage = new CritWriteVoteMessage(key, isOk);
        Logger.criticalWriteVote(this.id, LoggerOperationType.SEND, key, isOk);
        this.mainL1Cache.tell(critWriteVoteOkMessage, this.getSelf());
    }

    @Override
    protected void handleCritWriteVoteMessage(CritWriteVoteMessage message) {
        /*
        Do nothing here, L2 only sends vote messages
         */
    }

    @Override
    protected void handleCritWriteAbortMessage(CritWriteAbortMessage message) {
        this.resetWriteConfig(message.getKey());
    }

    @Override
    protected void handleCritWriteCommitMessage(CritWriteCommitMessage message) {
        // just update the value
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();

        // response to client if needed
        if (this.isWriteUnconfirmed(key)) {
            ActorRef client = this.unconfirmedWrites.get(key);
            if (this.isPrimaryL2ForCritWrite && client != ActorRef.noSender()) {
                WriteConfirmMessage confirmMessage = new WriteConfirmMessage(key, value, updateCount);
                Logger.writeConfirm(this.id, LoggerOperationType.SEND, key, value, 0, updateCount, 0);
                client.tell(confirmMessage, this.getSelf());
                // reset
                this.isPrimaryL2ForCritWrite = false;
            }
        }

        // reset critical write
        this.resetWriteConfig(key);
    }

    @Override
    protected boolean isCritWriteOk(int key) {
        if (this.isPrimaryL2ForCritWrite) {
            return !this.data.isLocked(key) && this.isWriteUnconfirmed(key);
        } else {
            return !this.data.isLocked(key) && !this.isWriteUnconfirmed(key);
        }
    }

    @Override
    protected boolean isL1Cache() {
        return false;
    }

    @Override
    protected void flush() {
        super.flush();
        this.isPrimaryL2ForCritWrite = false;
    }

    /**
     * Sends a ReadReply message to the saved sender. A ReadReply message is only send
     * back to the client. Therefore, no need to start a timeout, since a client is
     * not supposed to crash.
     *
     * @param key The key received by the ReadMessage
     */
    @Override
    protected void handleFill(int key) {
        if (this.isReadUnconfirmed(key)) {
            int value = this.data.getValueForKey(key).get();
            int updateCount = this.data.getUpdateCountForKey(key).get();

            // multicast to clients who have requested the key
            List<ActorRef> clients = this.unconfirmedReads.get(key);
            ReadReplyMessage readReplyMessage = new ReadReplyMessage(key, value, updateCount);
            Logger.readReply(this.id, LoggerOperationType.MULTICAST, key, value, 0, updateCount, 0);
            this.multicast(readReplyMessage, clients);
            // reset for key
            this.resetReadConfig(key);
        }
    }
}
