package it.unitn.disi.ds1.multi_level_cache.messages;

import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;

public class WriteMessage extends CrashableMessage {

    private final int key;
    private final int value;

    public WriteMessage(int key, int value, CacheCrashConfig l1CrashConfig, CacheCrashConfig l2CrashConfig) {
        super(l1CrashConfig, l2CrashConfig);
        this.key = key;
        this.value = value;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

}
