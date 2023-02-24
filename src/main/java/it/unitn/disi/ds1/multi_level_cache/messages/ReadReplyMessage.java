package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;

public class ReadReplyMessage implements Serializable {

    private final int key;

    private final int value;

    public ReadReplyMessage(int key, int value) {
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
