package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;

public class JoinMainL1CacheMessage {

    private final ActorRef l1Cache;

    public JoinMainL1CacheMessage(ActorRef l1Cache) {
        this.l1Cache = l1Cache;
    }

    public ActorRef getL1Cache() {
        return l1Cache;
    }

}
