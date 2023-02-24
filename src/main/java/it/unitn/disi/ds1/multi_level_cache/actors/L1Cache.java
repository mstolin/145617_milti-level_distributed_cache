package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

public class L1Cache extends Cache {

    private List<ActorRef> l2Caches;
    private ActorRef database;

    public L1Cache(int id) {
        super(String.format("L1-%d", id));
    }

    static public Props props(int id) {
        return Props.create(L1Cache.class, () -> new L1Cache(id));
    }

}
