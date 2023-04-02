package it.unitn.disi.ds1.multi_level_cache.actors;

import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;

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
        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, MessageType.WRITE, key, false, "Can't read value, because it's locked");
            this.sendLockedErrorToSender(key, MessageType.WRITE);
        } else {
            this.handleWriteMessage(message);
        }
    }

    protected void onCritWriteMessage(CritWriteMessage message) {
        int key = message.getKey();
        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, MessageType.CRITICAL_WRITE, key, false, "Can't read value, because it's locked");
            this.sendLockedErrorToSender(key, MessageType.CRITICAL_WRITE);
        } else {
            this.handleCritWriteMessage(message);
        }
    }

    protected void onCritWriteVoteMessage(CritWriteVoteMessage message) {
        this.handleCritWriteVoteMessage(message);
    }

    protected void onReadMessage(ReadMessage message) {
        int key = message.getKey();
        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, MessageType.READ, key, false, "Can't read value, because it's locked");
            this.sendLockedErrorToSender(key, MessageType.READ);
        } else {
            this.handleReadMessage(message);
        }
    }

    protected void onCritReadMessage(CritReadMessage message) {
        int key = message.getKey();
        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, MessageType.CRITICAL_READ, key, false, "Can't read value, because it's locked");
            this.sendLockedErrorToSender(key, MessageType.CRITICAL_READ);
        } else {
            this.handleCritReadMessage(message);
        }
    }

}
