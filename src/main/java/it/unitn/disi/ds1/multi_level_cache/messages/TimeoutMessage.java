package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;

import java.io.Serializable;

public class TimeoutMessage implements Serializable {

    private final Serializable message;
    private final ActorRef unreachableActor;
    private final MessageType type;

    public TimeoutMessage(Serializable message, ActorRef unreachableActor, MessageType type) {
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

    public MessageType getType() {
        return type;
    }

}
