package it.unitn.disi.ds1.multi_level_cache.messages;

import java.util.UUID;

public class CritWriteAbortMessage extends UUIDMessage {

    private final int key;

    public CritWriteAbortMessage(UUID uuid, int key) {
        super(uuid);
        this.key = key;
    }

    public int getKey() {
        return key;
    }

}
