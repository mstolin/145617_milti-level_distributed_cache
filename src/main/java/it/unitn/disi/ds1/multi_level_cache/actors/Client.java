package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.List;

public class Client extends AbstractActor {

    /** List of level 2 caches, the client knows about */
    private List<ActorRef> l2Caches;

    public Client() {
    }

    static public Props props() {
        return Props.create(Client.class, () -> new Client());
    }

    @Override
    public Receive createReceive() {
        return this.receiveBuilder().build();
    }

}
