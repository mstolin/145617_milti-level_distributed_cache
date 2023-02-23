package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;
import java.util.UUID;

public class WriteConfirmMessage implements Serializable {

    private final UUID writeMessageUUID;
    private final int key;
    private final int value;

    public WriteConfirmMessage(UUID writeMessageUUID, int key, int value) {
        this.writeMessageUUID = writeMessageUUID;
        this.key = key;
        this.value = value;
    }

    public UUID getWriteMessageUUID() {
        return writeMessageUUID;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }
}
