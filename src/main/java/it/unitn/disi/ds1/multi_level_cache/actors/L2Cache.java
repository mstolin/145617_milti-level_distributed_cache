package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class L2Cache extends AbstractActor {

    final Integer id;

    public L2Cache(int id) {
        this.id = id;
    }

    static public Props props(int id) {
        return Props.create(L2Cache.class, () -> new L2Cache(id));
    }

    @Override
    public Receive createReceive() {
        return this.receiveBuilder().build();
    }

}
