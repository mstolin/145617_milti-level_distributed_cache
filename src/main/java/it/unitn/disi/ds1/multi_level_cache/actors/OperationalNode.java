package it.unitn.disi.ds1.multi_level_cache.actors;

import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;

public abstract class OperationalNode extends Node {

    public OperationalNode(String id) {
        super(id);
    }

    private void sendLockedErrorToSender(int key, MessageType messageType) {
        ErrorMessage errorMessage = ErrorMessage.lockedKey(key, messageType);
        this.send(errorMessage, this.getSender());
    }

    protected abstract void handleWriteMessage(WriteMessage message);

    protected abstract void handleCritWriteMessage(CritWriteMessage message);

    protected abstract void handleCritWriteVoteMessage(CritWriteVoteMessage message);

    protected abstract void handleReadMessage(ReadMessage message);

    protected abstract void handleCritReadMessage(CritReadMessage message);

    protected void onWriteMessage(WriteMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        Logger.write(this.id, LoggerOperationType.RECEIVED, key, value, this.isKeyLocked(key));

        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, LoggerOperationType.SEND, MessageType.WRITE, key, false, "Can't read value, because it's locked");
            this.sendLockedErrorToSender(key, MessageType.WRITE);
        } else {
            this.handleWriteMessage(message);
        }
    }

    protected void onCritWriteMessage(CritWriteMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        Logger.criticalWrite(this.id, LoggerOperationType.RECEIVED, key, value, this.isKeyLocked(key));

        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, LoggerOperationType.SEND, MessageType.CRITICAL_WRITE, key, false, "Can't read value, because it's locked");
            this.sendLockedErrorToSender(key, MessageType.CRITICAL_WRITE);
        } else {
            this.handleCritWriteMessage(message);
        }
    }

    protected void onCritWriteVoteMessage(CritWriteVoteMessage message) {
        Logger.criticalWriteVote(this.id, LoggerOperationType.RECEIVED, message.getKey(), message.isOk());
        this.handleCritWriteVoteMessage(message);
    }

    protected void onReadMessage(ReadMessage message) {
        int key = message.getKey();
        Logger.read(this.id, LoggerOperationType.RECEIVED, key, message.getUpdateCount(), this.getUpdateCountOrElse(key),
                this.isKeyLocked(key), false, this.isReadUnconfirmed(key)); // todo is older is not always false here

        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, LoggerOperationType.SEND, MessageType.READ, key, false, "Can't read value, because it's locked");
            this.sendLockedErrorToSender(key, MessageType.READ);
        } else {
            this.handleReadMessage(message);
        }
    }

    protected void onCritReadMessage(CritReadMessage message) {
        int key = message.getKey();
        Logger.criticalRead(this.id, LoggerOperationType.RECEIVED, key, message.getUpdateCount(),
                this.getUpdateCountOrElse(key), this.isKeyLocked(key));

        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, LoggerOperationType.SEND, MessageType.CRITICAL_READ, key, false, "Can't read value, because it's locked");
            this.sendLockedErrorToSender(key, MessageType.CRITICAL_READ);
        } else {
            this.handleCritReadMessage(message);
        }
    }

}
