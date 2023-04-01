package it.unitn.disi.ds1.multi_level_cache.messages;

import it.unitn.disi.ds1.multi_level_cache.messages.utils.ErrorType;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

import java.io.Serializable;

public class ErrorMessage implements Serializable {

    private final ErrorType errorType;
    private final int key;
    private final TimeoutType messageType;

    public ErrorMessage(ErrorType errorType, int key, TimeoutType messageType) {
        this.errorType = errorType;
        this.key = key;
        this.messageType = messageType;
    }

    public static ErrorMessage unknownKey(int key, TimeoutType messageType) {
        return new ErrorMessage(ErrorType.UNKNOWN_KEY, key, messageType);
    }

    public static ErrorMessage lockedKey(int key, TimeoutType messageType) {
        return new ErrorMessage(ErrorType.LOCKED_KEY, key, messageType);
    }

    public ErrorType getErrorType() {
        return this.errorType;
    }

    public int getKey() {
        return this.key;
    }

    public TimeoutType getMessageType() {
        return this.messageType;
    }
}
