package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.List;

public class L1Cache extends AbstractActor {

    private Integer id;
    private List<ActorRef> l2Caches;
    private ActorRef database;

    public L1Cache(int id) {
        this.id = id;
    }

    static public Props props(int id) {
        return Props.create(L1Cache.class, () -> new L1Cache(id));
    }

    private void onJoinL2Cache(JoinL2CacheMessage message) {
        this.l2Caches = message.l2Caches;
        System.out.printf("L1 Cache %d joined group of %d L2 caches\n", this.id, this.l2Caches.size());
    }

    private void onJoinDatabase(JoinDatabaseMessage message) {
        this.database = message.database;
        System.out.printf("L1 Cache %d joined group of database\n", this.id);
    }

    @Override
    public Receive createReceive() {
        return this.receiveBuilder()
                .match(JoinL2CacheMessage.class, this::onJoinL2Cache)
                .match(JoinDatabaseMessage.class, this::onJoinDatabase)
                .build();
    }

    public static class JoinL2CacheMessage implements Serializable {
        private final List<ActorRef> l2Caches;

        public JoinL2CacheMessage(List<ActorRef> l2Caches) {
            // copyOf also returns an unmodifiable list
            this.l2Caches = List.copyOf(l2Caches);
        }
    }

    public static class JoinDatabaseMessage implements Serializable {
        private final ActorRef database;

        public JoinDatabaseMessage(ActorRef database) {
            this.database = database;
        }
    }
}
