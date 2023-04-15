package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;

import java.io.Serializable;
import java.util.List;

public class L2Cache extends Cache {

    public L2Cache(String id) {
        super(id);
    }

    static public Props props(String id) {
        return Props.create(Cache.class, () -> new L2Cache(id));
    }

    @Override
    protected void forwardMessageToNext(Serializable message, MessageType messageType) {
        if (message instanceof Message) {
            Message msg = (Message) message;
            if (msg.isMessageDelayedAtL2()) {
                this.send(message, this.mainL1Cache, msg.getL2MessageDelay());
                //this.setTimeout(message, this.mainL1Cache, messageType);
                return;
            }
        }

        this.send(message, this.mainL1Cache);
        this.setTimeout(message, this.mainL1Cache, messageType);
    }

    @Override
    protected void handleRefillMessage(RefillMessage message) {
        int key = message.getKey();
        if (this.isWriteUnconfirmed(key)) {
            // tell client write confirm
            ActorRef client = this.getUnconfirmedActorForWrit(key);
            int value = message.getValue();
            int updateCount = message.getUpdateCount();
            WriteConfirmMessage confirmMessage = new WriteConfirmMessage(key, value, updateCount);
            Logger.writeConfirm(this.id, LoggerOperationType.SEND, key, value, 0, updateCount, 0);
            this.send(confirmMessage, client);
            // reset timeout
            this.abortWrite(key);
        }
    }

    @Override
    protected void handleTimeoutMessage(TimeoutMessage message) {
        // forward message to DB, no need for timeout since DB can't timeout
        if (message.getType() == MessageType.READ) {
            ReadMessage readMessage = (ReadMessage) message.getMessage();
            int key = readMessage.getKey();

            // if the key is in this map, then no ReadReply has been received for the key
            if (this.isReadUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                Logger.read(this.id, LoggerOperationType.SEND, key, readMessage.getUpdateCount(),
                        this.getUpdateCountOrElse(key), this.isKeyLocked(key), false, true);
                this.send(readMessage, this.database);
            }
        } else if (message.getType() == MessageType.CRITICAL_READ) {
            CritReadMessage critReadMessage = (CritReadMessage) message.getMessage();
            int key = critReadMessage.getKey();

            // if the key is in this map, then no ReadReply has been received for the key
            if (this.isReadUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                Logger.criticalRead(this.id, LoggerOperationType.SEND, key, critReadMessage.getUpdateCount(),
                        this.getUpdateCountOrElse(key), this.isKeyLocked(key));
                this.send(critReadMessage, this.database);
            }
        } else if (message.getType() == MessageType.WRITE) {
            WriteMessage writeMessage = (WriteMessage) message.getMessage();
            int key = writeMessage.getKey();

            if (this.isWriteUnconfirmed(key)) {
                Logger.timeout(this.id, message.getType());
                Logger.write(this.id, LoggerOperationType.SEND, key, writeMessage.getValue(), this.isKeyLocked(key));
                this.send(writeMessage, this.database);
            }
        } else if (message.getType() == MessageType.CRITICAL_WRITE) {
            CritWriteMessage writeMessage = (CritWriteMessage) message.getMessage();
            int key = writeMessage.getKey();

            if (this.isWriteUnconfirmed(key)) {
                // do not forward to DB when crit write fails
                Logger.timeout(this.id, message.getType());
                this.abortCritWrite(key, false);
            }
        }
    }

    @Override
    protected void handleCritWriteRequestMessage(CritWriteRequestMessage message, boolean isOk) {
        int key = message.getKey();
        if (isOk) {
            // just lock data
            this.lockKey(key);
        }
        // answer back
        CritWriteVoteMessage critWriteVoteOkMessage = new CritWriteVoteMessage(key, isOk);
        Logger.criticalWriteVote(this.id, LoggerOperationType.SEND, key, isOk);
        this.send(critWriteVoteOkMessage, this.mainL1Cache);
    }

    @Override
    protected void handleCritWriteVoteMessage(CritWriteVoteMessage message) {
        /*
        Do nothing here, L2 only sends vote messages
         */
    }

    @Override
    protected void handleCritWriteAbortMessage(CritWriteAbortMessage message) {
        this.abortWrite(message.getKey());
    }

    @Override
    protected void handleCritWriteCommitMessage(CritWriteCommitMessage message) {
        // just update the value
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();

        // response write-confirm to client if needed
        if (this.isWriteUnconfirmed(key)) {
            ActorRef client = this.getUnconfirmedActorForWrit(key);
            WriteConfirmMessage confirmMessage = new WriteConfirmMessage(key, value, updateCount);
            Logger.writeConfirm(this.id, LoggerOperationType.SEND, key, value, 0, updateCount, 0);
            this.send(confirmMessage, client);
        }

        // reset critical write
        this.abortCritWrite(key, false);
    }

    @Override
    protected void handleErrorMessage(ErrorMessage message) {
        MessageType messageType = message.getMessageType();
        int key = message.getKey();

        if (messageType == MessageType.WRITE && this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, LoggerOperationType.SEND, messageType, key, false, "Forward error message");
            // tell L2 about message
            ActorRef client = this.getUnconfirmedActorForWrit(key);
            this.send(message, client);
            // reset
            this.abortWrite(key);
        } else if (messageType == MessageType.CRITICAL_WRITE && !this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, LoggerOperationType.SEND, messageType, key, false, "Forward error message");
            // tell L2 about message
            ActorRef client = this.getUnconfirmedActorForWrit(key);
            this.send(message, client);
            // reset and just timeout
            this.abortCritWrite(key, false);
        } else if ((messageType == MessageType.READ || messageType == MessageType.CRITICAL_READ) && this.isReadUnconfirmed(key)) {
            if (messageType == MessageType.READ) {
                Logger.error(this.id, LoggerOperationType.MULTICAST, messageType, key, false, "Forward error message");
            } else {
                Logger.error(this.id, LoggerOperationType.MULTICAST, messageType, key, false, "Forward error message");
            }

            // tell L2 about message
            List<ActorRef> clients = this.getUnconfirmedActorsForRead(key);
            this.multicast(message, clients);
            // reset
            this.removeUnconfirmedRead(key);
        }
    }

    @Override
    protected boolean isCritWriteOk(int key) {
        if (this.isWriteUnconfirmed(key)) {
            /*
            This is the L2 contacted by the client. The write is already unconfirmed.
            Therefore, only need to check if key is locked.
             */
            return !this.isKeyLocked(key);
        } else {
            /*
            This L2 has not been contacted by the client. Therefore, the write can't
            be unconfirmed by another request and must be unlocked.
             */
            return !this.isKeyLocked(key) && !this.isWriteUnconfirmed(key);
        }
    }

    @Override
    protected void abortCritWrite(int key, boolean multicastAbort) {
        // send error to client
        if (this.isWriteUnconfirmed(key)) {
            ActorRef client = this.getUnconfirmedActorForWrit(key);
            ErrorMessage errorMessage = ErrorMessage.internalError(key, MessageType.CRITICAL_WRITE);
            Logger.error(this.id, LoggerOperationType.SEND, MessageType.ERROR, key, false, "");
            this.send(errorMessage, client);
        }

        this.abortWrite(key);
    }

    @Override
    protected boolean isL1Cache() {
        return false;
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
            int value = this.getValueOrElse(key);
            int updateCount = this.getUpdateCountOrElse(key);

            // multicast to clients who have requested the key
            List<ActorRef> clients = this.getUnconfirmedActorsForRead(key);
            ReadReplyMessage readReplyMessage = new ReadReplyMessage(key, value, updateCount);
            Logger.readReply(this.id, LoggerOperationType.MULTICAST, key, value, 0, updateCount, 0);
            this.multicast(readReplyMessage, clients);
            // reset for key
            this.removeUnconfirmedRead(key);
        }
    }
}
