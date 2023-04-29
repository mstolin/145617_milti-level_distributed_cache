package it.unitn.disi.ds1.multi_level_cache.messages;

import it.unitn.disi.ds1.multi_level_cache.messages.utils.ErrorType;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;

import java.io.Serializable;

public class ErrorMessage implements Serializable {

    private final ErrorType errorType;
    private final int key;
    private final MessageType messageType;
    private final String errorMessage;

    public ErrorMessage(ErrorType errorType, int key, MessageType messageType, String errorMessage) {
        this.errorType = errorType;
        this.key = key;
        this.messageType = messageType;
        this.errorMessage = errorMessage;
    }

    public static ErrorMessage unknownKey(int key, MessageType messageType, String errorMessage) {
        return new ErrorMessage(ErrorType.UNKNOWN_KEY, key, messageType, errorMessage);
    }

    public static ErrorMessage lockedKey(int key, MessageType messageType, String errorMessage) {
        return new ErrorMessage(ErrorType.LOCKED_KEY, key, messageType, errorMessage);
    }

    public static ErrorMessage internalError(int key, MessageType messageType, String errorMessage) {
        return new ErrorMessage(ErrorType.INTERNAL_ERROR, key, messageType, errorMessage);
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

    public String getErrorMessage() {
        return this.errorMessage;
    }

}
