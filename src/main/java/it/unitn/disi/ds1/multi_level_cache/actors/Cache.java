package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class Cache extends AbstractActor {

    private final Integer id;

    public Cache(int id) {
        this.id = id;
    }

    static public Props props(int id) {
        return Props.create(Cache.class, () -> new Cache(id));
    }

    @Override
    public Receive createReceive() {
        return null;
    }

}
