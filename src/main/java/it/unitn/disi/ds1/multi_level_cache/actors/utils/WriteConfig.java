package it.unitn.disi.ds1.multi_level_cache.actors.utils;

import akka.actor.ActorRef;
import akka.japi.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class WriteConfig {

    private final Map<UUID, Pair<Integer, ActorRef>> unconfirmedWrites = new HashMap<>();

    public boolean isWriteUnconfirmed(int key) {
        return this.unconfirmedWrites
                .values()
                .stream()
                .anyMatch(pair -> pair.first() == key);
    }

    public boolean isWriteUUIDUnconfirmed(UUID uuid) {
        return this.unconfirmedWrites.containsKey(uuid);
    }

    public void addUnconfirmedWrite(UUID uuid, int key, ActorRef actor) {
        if (!this.isWriteUnconfirmed(key) && !this.isWriteUUIDUnconfirmed(uuid)) {
            this.unconfirmedWrites.put(uuid, Pair.create(key, actor));
        }
    }

    public void removeUnconfirmedWrite(UUID uuid) {
        if (this.isWriteUUIDUnconfirmed(uuid)) {
            this.unconfirmedWrites.remove(uuid);
        }
    }

    public void removeUnconfirmedWrite(int key) {
        if (this.isWriteUnconfirmed(key)) {
            Optional<UUID> uuid = this.getUnconfirmedUUID(key);
            if (uuid.isPresent()) {
                this.removeUnconfirmedWrite(uuid.get());
            }
        }
    }

    public Optional<UUID> getUnconfirmedUUID(int key) {
        for (Map.Entry<UUID, Pair<Integer, ActorRef>> entry : this.unconfirmedWrites.entrySet()) {
            if (entry.getValue().first() == key) {
                return Optional.of(entry.getKey());
            }
        }
        return Optional.empty();
    }

    public ActorRef getUnconfirmedActor(int key) {
        if (this.isWriteUnconfirmed(key)) {
            Optional<UUID> uuid = this.getUnconfirmedUUID(key);
            if (uuid.isPresent()) {
                return this.getUnconfirmedActor(uuid.get());
            }
        }
        return ActorRef.noSender();
    }

    public ActorRef getUnconfirmedActor(UUID uuid) {
        if (this.isWriteUUIDUnconfirmed(uuid)) {
            return this.unconfirmedWrites.get(uuid).second();
        }
        return ActorRef.noSender();
    }

}
