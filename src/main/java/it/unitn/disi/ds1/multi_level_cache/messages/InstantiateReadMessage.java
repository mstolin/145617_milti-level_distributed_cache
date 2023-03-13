package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;

import java.io.Serializable;

public class InstantiateReadMessage implements Serializable {

    private final int key;

    private final boolean isCritical;

    private final ActorRef l2Cache;

    public InstantiateReadMessage(int key, ActorRef l2Cache, boolean isCritical) {
        this.key = key;
        this.l2Cache = l2Cache;
        this.isCritical = isCritical;
    }

    public int getKey() {
        return key;
    }

    public ActorRef getL2Cache() {
        return l2Cache;
    }

    public boolean isCritical() {
        return isCritical;
    }
    
}
