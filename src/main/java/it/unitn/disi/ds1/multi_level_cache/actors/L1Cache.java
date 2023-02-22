package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinActorMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinGroupMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteMessage;

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

    private void onJoinL2Cache(JoinGroupMessage message) {
        this.l2Caches = List.copyOf(message.getGroup());
        System.out.printf("L1 Cache %d joined group of %d L2 caches\n", this.id, this.l2Caches.size());
    }

    private void onJoinDatabase(JoinActorMessage message) {
        this.database = message.getActor();
        System.out.printf("L1 Cache %d joined group of database\n", this.id);
    }

    private void onWriteMessage(WriteMessage message) {
        // just forward message for now
        System.out.println("L1 hat bekommen\n");
        this.database.tell(message, getSelf());
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinGroupMessage.class, this::onJoinL2Cache)
                .match(JoinActorMessage.class, this::onJoinDatabase)
                .match(WriteMessage.class, this::onWriteMessage)
                .build();
    }
}
