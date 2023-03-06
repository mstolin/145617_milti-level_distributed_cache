package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

import java.io.Serializable;

public class TimeoutMessage implements Serializable {

    private final Serializable message;
    private final ActorRef unreachableActor;
    private final TimeoutType type;

    public TimeoutMessage(Serializable message, ActorRef unreachableActor, TimeoutType type) {
        this.message = message;
        this.unreachableActor = unreachableActor;
        this.type = type;
    }

    public Serializable getMessage() {
        return this.message;
    }

    public ActorRef getUnreachableActor() {
        return this.unreachableActor;
    }

    public TimeoutType getType() {
        return type;
    }

}
