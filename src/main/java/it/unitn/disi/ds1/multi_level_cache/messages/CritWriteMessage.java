package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;

public class CritWriteMessage implements Serializable {

    private final int key;

    private final int value;


    public CritWriteMessage(int key, int value) {
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
