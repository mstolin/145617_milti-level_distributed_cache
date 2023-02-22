package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;

import java.io.Serializable;

public class JoinActorMessage implements Serializable {
    private final ActorRef actor;

    public JoinActorMessage(ActorRef actor) {
        this.actor = actor;
    }
}
