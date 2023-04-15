package it.unitn.disi.ds1.multi_level_cache.messages;

import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageConfig;

import java.io.Serializable;

public abstract class Message implements Serializable {

    protected final MessageConfig messageConfig;

    protected Message(MessageConfig messageConfig) {
        this.messageConfig = messageConfig;
    }

    public CacheCrashConfig getL1CrashConfig() {
        return this.messageConfig.getL1CrashConfig();
    }

    public CacheCrashConfig getL2CrashConfig() {
        return this.messageConfig.getL2CrashConfig();
    }

    public long getDelay() {
        return this.messageConfig.getDelay();
    }

}
