package it.unitn.disi.ds1.multi_level_cache.messages;

import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageConfig;

import java.util.UUID;

public abstract class Message extends UUIDMessage {

    protected final MessageConfig messageConfig;

    protected Message(MessageConfig messageConfig) {
        super();
        this.messageConfig = messageConfig;
    }

    protected Message(UUID uuid, MessageConfig messageConfig) {
        super(uuid);
        this.messageConfig = messageConfig;
    }

    public MessageConfig getMessageConfig() {
        return this.messageConfig;
    }

    public boolean isMessageDelayedAtL1() {
        return this.messageConfig.isMessageDelayedAtL1();
    }

    public boolean isMessageDelayedAtL2() {
        return this.messageConfig.isMessageDelayedAtL2();
    }

    public long getL1MessageDelay() {
        return this.messageConfig.getL1MessageDelay();
    }

    public long getL2MessageDelay() {
        return this.messageConfig.getL2MessageDelay();
    }

    public boolean mustL1Crash() {
        return this.messageConfig.mustL1Crash();
    }

    public boolean mustL2Crash() {
        return this.messageConfig.mustL2Crash();
    }

    public long getL1RecoverDelay() {
        return this.messageConfig.getL1RecoverDelay();
    }

    public long getL2RecoverDelay() {
        return this.messageConfig.getL2RecoverDelay();
    }

}
