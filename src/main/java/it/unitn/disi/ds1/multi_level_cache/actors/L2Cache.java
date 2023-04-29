package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.ErrorType;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class L2Cache extends Cache {

    public L2Cache(String id) {
        super(id);
    }

    static public Props props(String id) {
        return Props.create(Cache.class, () -> new L2Cache(id));
    }

    private void abortCritWriteAnd(UUID uuid, int key, boolean sendErrorToClient) {
        // send error to client
        if (this.isWriteUnconfirmed(key) && sendErrorToClient) {
            ActorRef client = this.getUnconfirmedActorForWrit(uuid);
            String errMsg = "Aborted Critical Write";
            ErrorMessage errorMessage = ErrorMessage.internalError(key, MessageType.CRITICAL_WRITE, errMsg);
            Logger.error(this.id, LoggerOperationType.SEND, MessageType.ERROR, key, false, errMsg);
            this.send(errorMessage, client);
        }

        this.abortCritWrite(uuid, key);
    }

    @Override
    protected void forwardMessageToNext(Serializable message, MessageType messageType, long millis) {
        long messageDelay = 0;

        if (message instanceof Message msg) {
            if (msg.isMessageDelayedAtL2()) {
                messageDelay = msg.getL2MessageDelay();
            }
        }

        this.send(message, this.mainL1Cache, messageDelay);
        this.setTimeout(message, this.mainL1Cache, messageType, millis);
    }

    @Override
    protected void forwardMessageToNext(Serializable message, MessageType messageType) {
        this.forwardMessageToNext(message, messageType, this.getTimeoutMillis());
    }

    @Override
    protected void handleRefillMessage(RefillMessage message) {
        this.abortWrite(message.getUuid(), message.getKey());
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

                Optional<UUID> uuid = this.getUnconfirmedWriteUUID(key);
                if (uuid.isPresent()) {
                    // send error to client
                    String errMsg = "L1 is unreachable";
                    Logger.error(this.id, LoggerOperationType.ERROR, message.getType(), key, false, errMsg);
                    ErrorMessage errorMessage = ErrorMessage.internalError(key, MessageType.WRITE, errMsg);
                    ActorRef client = this.getUnconfirmedActorForWrit(uuid.get());
                    this.send(errorMessage, client);

                    // abort
                    this.abortWrite(uuid.get(), key);
                }
            }
        } else if (message.getType() == MessageType.CRITICAL_WRITE) {
            CritWriteMessage writeMessage = (CritWriteMessage) message.getMessage();
            int key = writeMessage.getKey();

            if (this.isWriteUnconfirmed(key)) {
                // do not forward to DB when crit write fails
                Logger.timeout(this.id, message.getType());
                this.abortCritWriteAnd(writeMessage.getUuid(), key, true);
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
        CritWriteVoteMessage critWriteVoteOkMessage = new CritWriteVoteMessage(message.getUuid(), key, isOk);
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
        this.abortCritWriteAnd(message.getUuid(), message.getKey(), true);
    }

    @Override
    protected void handleCritWriteCommitMessage(CritWriteCommitMessage message) {
        // just update the value
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();

        if (this.isWriteUnconfirmed(key)) {
            Optional<UUID> uuid = this.getUnconfirmedWriteUUID(key);
            if (uuid.isPresent()) {
                // response write-confirm to client if needed
                ActorRef client = this.getUnconfirmedActorForWrit(uuid.get());
                WriteConfirmMessage confirmMessage = new WriteConfirmMessage(key, value, updateCount, message.getUuid());
                Logger.writeConfirm(this.id, message.getUuid(), LoggerOperationType.SEND, key, value, 0, updateCount, 0);
                this.send(confirmMessage, client);

                // reset critical write
                this.abortCritWriteAnd(uuid.get(), key, false);
            }
        }
    }

    @Override
    protected void handleErrorMessage(ErrorMessage message) {
        MessageType messageType = message.getMessageType();
        int key = message.getKey();

        if (messageType == MessageType.WRITE && this.isWriteUnconfirmed(key)) {
            Optional<UUID> uuid = this.getUnconfirmedWriteUUID(key);
            if (uuid.isPresent()) {
                Logger.error(this.id, LoggerOperationType.SEND, messageType, key, false, "Forward error message");
                // tell L2 about message

                ActorRef client = this.getUnconfirmedActorForWrit(uuid.get());
                this.send(message, client);

                // reset
                this.abortWrite(uuid.get(), key);
            }
        } else if (messageType == MessageType.CRITICAL_WRITE && !this.isWriteUnconfirmed(key)) {
            Optional<UUID> uuid = this.getUnconfirmedWriteUUID(key);
            if (uuid.isPresent()) {
                Logger.error(this.id, LoggerOperationType.SEND, messageType, key, false, "Forward error message");
                // tell client about message
                ActorRef client = this.getUnconfirmedActorForWrit(uuid.get());
                this.send(message, client);

                // reset and just timeout
                this.abortCritWriteAnd(uuid.get(), key, false);
            }
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
    protected void abortCritWrite(UUID uuid, int key) {
        this.abortWrite(uuid, key);
    }

    @Override
    protected boolean isL1Cache() {
        return false;
    }

    @Override
    protected void sendWriteConfirm(UUID uuid, int key, int value, int updateCount) {
        if (this.isWriteUnconfirmed(key) && this.isWriteUUIDUnconfirmed(uuid)) {
            // tell client write confirm
            ActorRef client = this.getUnconfirmedActorForWrit(uuid);
            WriteConfirmMessage confirmMessage = new WriteConfirmMessage(key, value, updateCount, uuid);
            Logger.writeConfirm(this.id, uuid, LoggerOperationType.SEND, key, value, 0, updateCount, 0);
            this.send(confirmMessage, client);
        }
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
