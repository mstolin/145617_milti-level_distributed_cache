package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinGroupMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteMessage;

import java.util.List;

public class Client extends AbstractActor {

    /** List of level 2 caches, the client knows about */
    private List<ActorRef> l2Caches;

    public Client() {
    }

    static public Props props() {
        return Props.create(Client.class, () -> new Client());
    }

    private void onJoinL2Cache(JoinGroupMessage message) {
        this.l2Caches = List.copyOf(message.getGroup());
        System.out.printf("Client joined group of %d L2 caches\n", this.l2Caches.size());
    }

    private void onWriteMessage(WriteMessage message) {
        // just forward message for now
        System.out.println("Client hat bekommen\n");
        ActorRef l2Cache = this.l2Caches.get(0);
        l2Cache.tell(message, getSelf());
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinGroupMessage.class, this::onJoinL2Cache)
                .match(WriteMessage.class, this::onWriteMessage)
                .build();
    }

}
