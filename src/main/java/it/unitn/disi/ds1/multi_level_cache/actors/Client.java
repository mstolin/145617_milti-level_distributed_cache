package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinGroupMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteConfirmMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteMessage;

import java.util.ArrayList;
import java.util.List;

public class Client extends AbstractActor {

    /** List of level 2 caches, the client knows about */
    private List<ActorRef> l2Caches;
    private List<WriteMessage> writeHistory = new ArrayList<>();
    private List<WriteMessage> readHistory = new ArrayList<>();
    final String id;

    public Client(int id) {
        this.id = String.format("Client-%d", id);
    }

    static public Props props(int id) {
        return Props.create(Client.class, () -> new Client(id));
    }

    private void onJoinL2Cache(JoinGroupMessage message) {
        this.l2Caches = List.copyOf(message.getGroup());
        System.out.printf("%s joined group of %d L2 caches\n", this.id, this.l2Caches.size());
    }

    private void onWriteMessage(WriteMessage message) {
        System.out.printf("%s sends write message to random L2 cache\n", this.id);
        // just forward message for now
        this.writeHistory.add(message);
        ActorRef l2Cache = this.l2Caches.get(0);
        l2Cache.tell(message, getSelf());
    }

    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        System.out.printf("%s received write confirm\n", this.id);
        WriteMessage confirmed = message.getWriteMessage();
        if (this.writeHistory.remove(confirmed)) {
            System.out.println("MESSAGE WAS REMOVED FROM HISTORY");
        }
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinGroupMessage.class, this::onJoinL2Cache)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .build();
    }

}
