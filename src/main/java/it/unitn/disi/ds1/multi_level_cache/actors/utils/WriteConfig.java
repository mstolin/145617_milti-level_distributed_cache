package it.unitn.disi.ds1.multi_level_cache.actors.utils;

import akka.actor.ActorRef;

import java.util.HashMap;
import java.util.Map;

public class WriteConfig {

    private Map<Integer, ActorRef> unconfirmedWrites = new HashMap<>();

    public boolean isWriteUnconfirmed(int key) {
        return this.unconfirmedWrites.containsKey(key);
    }

    public void addUnconfirmedWrite(int key, ActorRef actor) {
        if (!this.isWriteUnconfirmed(key)) {
            this.unconfirmedWrites.put(key, actor);
        }
    }

    public void removeUnconfirmedWrite(int key) {
        if (this.isWriteUnconfirmed(key)) {
            this.unconfirmedWrites.remove(key);
        }
    }

    public ActorRef getUnconfirmedActor(int key) {
        if (this.isWriteUnconfirmed(key)) {
            return this.unconfirmedWrites.get(key);
        }
        return ActorRef.noSender();
    }

}
