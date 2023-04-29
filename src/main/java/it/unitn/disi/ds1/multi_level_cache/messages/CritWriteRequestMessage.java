package it.unitn.disi.ds1.multi_level_cache.messages;

import java.util.UUID;

public class CritWriteRequestMessage extends UUIDMessage {

    private final int key;

    public CritWriteRequestMessage(UUID uuid, int key) {
        super(uuid);
        this.key = key;
    }

    public int getKey() {
        return key;
    }

}
