package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;

import java.io.Serializable;

public class FlushMessage implements Serializable {

    private final ActorRef crashedNode;

    public FlushMessage(ActorRef crashedNode) {
        this.crashedNode = crashedNode;
    }

    public ActorRef getCrashedNode() {
        return crashedNode;
    }

}
