package it.unitn.disi.ds1.multi_level_cache.messages;

import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;

import java.io.Serializable;

public abstract class CrashableMessage implements Serializable {

    protected final CacheCrashConfig l1CrashConfig;
    protected final CacheCrashConfig l2CrashConfig;

    protected CrashableMessage(CacheCrashConfig l1CrashConfig, CacheCrashConfig l2CrashConfig) {
        this.l1CrashConfig = l1CrashConfig;
        this.l2CrashConfig = l2CrashConfig;
    }

    public CacheCrashConfig getL1CrashConfig() {
        return l1CrashConfig;
    }

    public CacheCrashConfig getL2CrashConfig() {
        return l2CrashConfig;
    }

}
