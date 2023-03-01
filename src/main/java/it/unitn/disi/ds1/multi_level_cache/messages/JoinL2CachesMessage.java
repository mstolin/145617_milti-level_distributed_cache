package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

public class JoinL2CachesMessage implements Serializable {

    private final List<ActorRef> l2Caches;

    public JoinL2CachesMessage(List<ActorRef> l2Caches) {
        this.l2Caches = List.copyOf(l2Caches);
    }

    public List<ActorRef> getL2Caches() {
        return l2Caches;
    }

}
