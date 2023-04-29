package it.unitn.disi.ds1.multi_level_cache.messages;

import java.util.UUID;

public class CritWriteVoteMessage extends UUIDMessage {

    private final boolean isOk;

    private final int key;

    public CritWriteVoteMessage(UUID uuid, int key, boolean isOk) {
        super(uuid);
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
