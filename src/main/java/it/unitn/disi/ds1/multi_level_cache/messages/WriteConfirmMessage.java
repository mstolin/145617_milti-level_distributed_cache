package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;

public class WriteConfirmMessage implements Serializable {

    private final int key;
    private final int value;
    private final int updateCount;

    public WriteConfirmMessage(int key, int value, int updateCount) {
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
