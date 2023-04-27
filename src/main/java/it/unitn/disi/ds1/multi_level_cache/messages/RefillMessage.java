package it.unitn.disi.ds1.multi_level_cache.messages;

import java.util.UUID;

public class RefillMessage extends UUIDMessage {

    private final int key;

    private final int value;

    private final int updateCount;

    public RefillMessage(UUID uuid, int key, int value, int updateCount) {
        super(uuid);
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
