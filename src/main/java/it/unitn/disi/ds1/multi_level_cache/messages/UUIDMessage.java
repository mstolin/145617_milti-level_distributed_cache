package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;
import java.util.UUID;

public class UUIDMessage implements Serializable {

    private final UUID uuid;

    protected UUIDMessage() {
        this.uuid = UUID.randomUUID();
    }

    protected UUIDMessage(UUID uuid) {
        this.uuid = uuid;
    }

    public UUID getUuid() {
        return this.uuid;
    }

}
