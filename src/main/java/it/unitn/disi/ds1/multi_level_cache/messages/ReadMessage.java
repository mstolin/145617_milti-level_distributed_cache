package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;

public class ReadMessage implements Serializable {

    private final int key;

    private final int updateCount;

    public ReadMessage(int key, int updateCount) {
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
