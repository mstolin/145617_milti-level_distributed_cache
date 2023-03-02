package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.io.Serializable;
import java.util.*;

public class Client extends AbstractActor {

    /** List of level 2 caches, the client knows about */
    private List<ActorRef> l2Caches;
    private DataStore data = new DataStore();
    private Map<Serializable, ActorRef> messageQueue = new HashMap<>();
    private Timer timer;
    final String id;

    public Client(String id) {
        this.id = id;
    }

    static public Props props(String id) {
        return Props.create(Client.class, () -> new Client(id));
    }

    private Optional<ActorRef> getAnotherL2Cache(ActorRef currentL2Cache) {
        for (ActorRef l2Cache: this.l2Caches) {
            if (l2Cache != currentL2Cache) {
                return Optional.of(l2Cache);
            }
        }
        return Optional.empty();
    }

    private void startTimeout(Serializable message) {
        this.timer = new Timer();
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                receiverTimedOut(message);
            }
        }, 4000);
    }

    private void stopTimeout() {
        this.timer.cancel();
        this.timer = null;
    }

    private void receiverTimedOut(Serializable message) {
        // just use another L2 cache
        Optional<ActorRef> l2Cache = this.getAnotherL2Cache(this.messageQueue.get(message));
        if (l2Cache.isPresent()) {
            // resend message
            System.out.printf("%s resend message to another L2\n", this.id);
            l2Cache.get().tell(message, this.getSelf());
            this.startTimeout(message);
        } else {
            // Seems like no actor is available
            System.out.printf("%s didn't found any available L2 caches\n", this.id);
            this.stopTimeout();
        }
    }

    private void onJoinL2Caches(JoinL2CachesMessage message) {
        this.l2Caches = List.copyOf(message.getL2Caches());
        System.out.printf("%s joined group of %d L2 caches\n", this.id, this.l2Caches.size());
    }

    private void onInstantiateWriteMessage(InstantiateWriteMessage message) {
        ActorRef l2Cache = message.getL2Cache();
        if (!this.l2Caches.contains(l2Cache)) {
            System.out.printf("%s The given L2 Cache is unknown\n", this.id);
            return;
        }
        System.out.printf("%s sends write message to L2 cache\n", this.id);

        // todo add l2 cache to some queue

        WriteMessage writeMessage = new WriteMessage(message.getKey(), message.getValue());
        l2Cache.tell(writeMessage, this.getSelf());
        this.startTimeout(writeMessage);
    }

    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        System.out.printf("%s received write confirm for key %d (new UC: %d, old UC: %d)\n",
                this.id, key, value, updateCount);
        // update value
        this.data.setValueForKey(key, value, updateCount);
        // todo remove write message from "to be confirmed queue"
        this.stopTimeout();
    }

    private void onInstantiateReadMessage(InstantiateReadMessage message) {
        ActorRef l2Cache = message.getL2Cache();
        if (!this.l2Caches.contains(l2Cache)) {
            System.out.printf("%s The given L2 Cache is unknown\n", this.id);
            return;
        }
        int key = message.getKey();
        System.out.printf("%s send read message to L2 Cache for key %d\n", this.id, key);

        ReadMessage readMessage = new ReadMessage(key, this.data.getUpdateCountForKey(key).orElse(0));
        this.messageQueue.put(readMessage, l2Cache);

        l2Cache.tell(readMessage, this.getSelf());
        this.startTimeout(readMessage);
    }

    private void onReadReplyMessage(ReadReplyMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        System.out.printf("%s Received read reply {%d: %d} (new UC: %d, old UC: %d)\n",
                this.id, key, value, updateCount, this.data.getUpdateCountForKey(key).orElse(0));
        // update value
        this.data.setValueForKey(key, value, updateCount);
        // todo remove read message from "to be confirmed queue"
        this.stopTimeout();
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinL2CachesMessage.class, this::onJoinL2Caches)
                .match(InstantiateWriteMessage.class, this::onInstantiateWriteMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .match(InstantiateReadMessage.class, this::onInstantiateReadMessage)
                .match(ReadReplyMessage.class, this::onReadReplyMessage)
                .build();
    }

}
