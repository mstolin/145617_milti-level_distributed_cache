package it.unitn.disi.ds1.multi_level_cache.messages;

import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;

public class ReadMessage extends CrashableMessage {

    private final int key;

    private final int updateCount;

    public ReadMessage(int key, int updateCount,
                       CacheCrashConfig l1CrashConfig, CacheCrashConfig l2CrashConfig) {
        super(l1CrashConfig, l2CrashConfig);
        this.key = key;
        this.updateCount = updateCount;
    }

    public int getKey() {
        return key;
    }

    public int getUpdateCount() {
        return updateCount;
    }

}
