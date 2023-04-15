package it.unitn.disi.ds1.multi_level_cache.messages;

import it.unitn.disi.ds1.multi_level_cache.messages.utils.ErrorType;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;

import java.io.Serializable;

public class ErrorMessage implements Serializable {

    private final ErrorType errorType;
    private final int key;
    private final MessageType messageType;

    public ErrorMessage(ErrorType errorType, int key, MessageType messageType) {
        this.errorType = errorType;
        this.key = key;
        this.messageType = messageType;
    }

    public static ErrorMessage unknownKey(int key, MessageType messageType) {
        return new ErrorMessage(ErrorType.UNKNOWN_KEY, key, messageType);
    }

    public static ErrorMessage lockedKey(int key, MessageType messageType) {
        return new ErrorMessage(ErrorType.LOCKED_KEY, key, messageType);
    }

    public static ErrorMessage internalError(int key, MessageType messageType) {
        return new ErrorMessage(ErrorType.INTERNAL_ERROR, key, messageType);
    }

    public ErrorType getErrorType() {
        return this.errorType;
    }

    public int getKey() {
        return this.key;
    }

    public MessageType getMessageType() {
        return this.messageType;
    }
}
