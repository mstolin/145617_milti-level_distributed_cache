package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;

import java.io.Serializable;

public class InstantiateWriteMessage implements Serializable {

    private final int key;

    private final int value;

    private final ActorRef l2Cache;

    private final boolean isCritical;

    public InstantiateWriteMessage(int key, int value, ActorRef l2Cache, boolean isCritical) {
        this.key = key;
        this.value = value;
        this.l2Cache = l2Cache;
        this.isCritical = isCritical;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    public ActorRef getL2Cache() {
        return l2Cache;
    }

    public boolean isCritical() {
        return isCritical;
    }

}
