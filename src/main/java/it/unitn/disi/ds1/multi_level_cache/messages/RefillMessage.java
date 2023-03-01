package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;

public class RefillMessage implements Serializable {

    private final int key;

    private final int value;

    private final int updateCount;

    public RefillMessage(int key, int value, int updateCount) {
        this.key = key;
        this.value = value;
        this.updateCount = updateCount;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    public int getUpdateCount() {
        return updateCount;
    }

}
