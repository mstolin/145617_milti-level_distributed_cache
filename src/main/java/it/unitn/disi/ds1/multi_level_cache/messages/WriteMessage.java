package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;

public class WriteMessage implements Serializable {

    private final int key;
    private final int value;

    public WriteMessage(int key, int value) {
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
