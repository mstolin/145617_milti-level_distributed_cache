package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;
import java.util.UUID;

public class WriteMessage implements Serializable {

    private final UUID uuid;
    private final int key;
    private final int value;

    public WriteMessage(int key, int value) {
        this.uuid = UUID.randomUUID();
        this.key = key;
        this.value = value;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    public UUID getUuid() {
        return uuid;
    }
}
