package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

public class JoinL1CachesMessage implements Serializable {

    private final List<ActorRef> l1Caches;

    public JoinL1CachesMessage(List<ActorRef> l1Caches) {
        this.l1Caches = List.copyOf(l1Caches);
    }

    public List<ActorRef> getL1Caches() {
        return l1Caches;
    }

}
