package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;

public class ReadMessage implements Serializable {

    private final int key;

    public ReadMessage(int key) {
        this.key = key;
    }

    public int getKey() {
        return key;
    }

}
