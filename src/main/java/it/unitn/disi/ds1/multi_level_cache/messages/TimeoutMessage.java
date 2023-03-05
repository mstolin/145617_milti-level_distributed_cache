package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;

import java.io.Serializable;

public class TimeoutMessage implements Serializable {

    private final Serializable message;
    private final ActorRef unreachableActor;

    public TimeoutMessage(Serializable message, ActorRef unreachableActor) {
        this.message = message;
        this.unreachableActor = unreachableActor;
    }

    public Serializable getMessage() {
        return this.message;
    }

    public ActorRef getUnreachableActor() {
        return this.unreachableActor;
    }

}
