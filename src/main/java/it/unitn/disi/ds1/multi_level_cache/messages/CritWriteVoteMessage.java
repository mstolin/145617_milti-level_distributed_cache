package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;

public class CritWriteVoteMessage implements Serializable {

    private final boolean isOk;

    private final int key;

    public CritWriteVoteMessage(int key, boolean isOk) {
        this.key = key;
        this.isOk = isOk;
    }

    public int getKey() {
        return key;
    }

    public boolean isOk() {
        return isOk;
    }

}
