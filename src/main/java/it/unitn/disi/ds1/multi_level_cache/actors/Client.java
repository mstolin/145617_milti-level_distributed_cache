package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

    private ActorRef getRandomL2Cache() {
        Random rand = new Random();
        ActorRef l2Cache = this.l2Caches.get(rand.nextInt(this.l2Caches.size()));
        return l2Cache;
    }

    private void onJoinL2Cache(JoinGroupMessage message) {
        this.l2Caches = List.copyOf(message.getGroup());
        System.out.printf("%s joined group of %d L2 caches\n", this.id, this.l2Caches.size());
    }

    private void onWriteMessage(WriteMessage message) {
        System.out.printf("%s sends write message to random L2 cache\n", this.id);
        ActorRef l2Cache = this.getRandomL2Cache();
        l2Cache.tell(message, this.getSelf());
    }

    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        System.out.printf("%s received write confirm (%s)\n", this.id, message.getWriteMessageUUID().toString());
    }

    private void onReadMessage(ReadMessage message) {
        System.out.printf("%s send read message to random L2 Cache\n", this.id);
        ActorRef l2Cache = this.getRandomL2Cache();
        l2Cache.tell(message, this.getSelf());
    }

    private void onReadReplyMessage(ReadReplyMessage message) {
        System.out.printf("Received read reply {%d: %d}\n", message.getKey(), message.getValue());
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinGroupMessage.class, this::onJoinL2Cache)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .match(ReadMessage.class, this::onReadMessage)
                .match(ReadReplyMessage.class, this::onReadReplyMessage)
                .build();
    }

}
