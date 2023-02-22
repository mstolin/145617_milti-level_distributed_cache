package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

public class JoinGroupMessage implements Serializable {

    private final List<ActorRef> group;

    public JoinGroupMessage(List<ActorRef> group) {
        // copyOf also returns an unmodifiable list
        this.group = List.copyOf(group);
    }

    public List<ActorRef> getGroup() {
        return group;
    }
}
