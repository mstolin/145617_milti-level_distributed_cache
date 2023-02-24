package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.io.Serializable;
import java.util.*;

public class L2Cache extends Cache {

    private ActorRef l1Cache;
    private Map<Integer, List<ActorRef>> readQueue = new HashMap<>();

    public L2Cache(int l1Id, int id) {
        super(String.format("L2-%d-%d", l1Id, id));
        this.isLastLevelCache = true;
    }

    static public Props props(int l1Id, int id) {
        return Props.create(L2Cache.class, () -> new L2Cache(l1Id, id));
    }

}
