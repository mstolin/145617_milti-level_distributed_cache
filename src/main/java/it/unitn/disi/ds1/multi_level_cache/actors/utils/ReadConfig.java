package it.unitn.disi.ds1.multi_level_cache.actors.utils;

import akka.actor.ActorRef;

import java.util.*;

public class ReadConfig {

    private Map<Integer, List<ActorRef>> unconfirmedReads = new HashMap<>();

    public boolean isReadUnconfirmed(int key) {
        return this.unconfirmedReads.containsKey(key);
    }

    public void addUnconfirmedRead(int key, ActorRef actor) {
        if (this.isReadUnconfirmed(key)) {
            // add to existing list
            this.unconfirmedReads.get(key).add(actor);
        } else {
            List<ActorRef> actors = new ArrayList<>();
            actors.add(actor);
            this.unconfirmedReads.put(key, actors);
        }
    }

    public void removeUnconfirmedRead(int key) {
        if (this.isReadUnconfirmed(key)) {
            this.unconfirmedReads.remove(key);
        }
    }

    public List<ActorRef> getUnconfirmedActors(int key) {
        if (this.isReadUnconfirmed(key)) {
            return this.unconfirmedReads.get(key);
        }
        return Collections.emptyList();
    }

}
