package it.unitn.disi.ds1.multi_level_cache.messages;

import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageConfig;

public class CritReadMessage extends Message {

    private final int key;

    private final int updateCount;

    public CritReadMessage(int key, int updateCount, MessageConfig messageConfig) {
        super(messageConfig);
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
