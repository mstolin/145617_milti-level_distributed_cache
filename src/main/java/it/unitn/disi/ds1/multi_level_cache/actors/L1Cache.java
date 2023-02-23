package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinActorMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinGroupMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteConfirmMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteMessage;

import java.util.List;

public class L1Cache extends AbstractActor {

    private String id;
    private List<ActorRef> l2Caches;
    private ActorRef database;

    public L1Cache(int id) {
        this.id = String.format("L1-%d", id);
    }

    static public Props props(int id) {
        return Props.create(L1Cache.class, () -> new L1Cache(id));
    }

    private void onJoinL2Cache(JoinGroupMessage message) {
        this.l2Caches = List.copyOf(message.getGroup());
        System.out.printf("%s joined group of %d L2 caches\n", this.id, this.l2Caches.size());
    }

    private void onJoinDatabase(JoinActorMessage message) {
        this.database = message.getActor();
        System.out.printf("%s joined group of database\n", this.id);
    }

    private void onWriteMessage(WriteMessage message) {
        // just forward message for now
        System.out.printf("%s received write message, forward to database\n", this.id);
        this.database.tell(message, getSelf());
    }

    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        System.out.printf("%s received write confirm, forward to L2 cache\n", this.id);
        this.getSender().tell(message, getSelf());
        /*WriteMessage confirmed = message.getWriteMessage();
        System.out.printf("MESSAGE CONFIRMED %d: %d\n", confirmed.getKey(), confirmed.getValue());
        if (this.writeHistory.remove(confirmed)) {
            System.out.println("MESSAGE WAS REMOVED FROM HISTORY");
        }*/
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinGroupMessage.class, this::onJoinL2Cache)
                .match(JoinActorMessage.class, this::onJoinDatabase)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .build();
    }
}
