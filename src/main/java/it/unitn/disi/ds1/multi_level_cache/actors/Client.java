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
    //private Map<Serializable, ActorRef> messageQueue = new HashMap<>();
    private Serializable currentMessage;
    private ActorRef currentMessageReceiver;
    private Timer timer;
    final String id;

    public Client(String id) {
        this.id = id;
    }

    static public Props props(String id) {
        return Props.create(Client.class, () -> new Client(id));
    }

    private boolean canInstantiateNewConversation() {
        return this.currentMessage != null || this.currentMessageReceiver != null;
    }

    private Optional<ActorRef> getAnotherL2Cache(ActorRef currentL2Cache) {
        for (ActorRef l2Cache: this.l2Caches) {
            if (l2Cache != currentL2Cache) {
                return Optional.of(l2Cache);
            }
        }
        return Optional.empty();
    }

    private void startTimeout() {
        this.timer = new Timer();
        // the delay has to be higher than the delay of the caches,
        // otherwise it will timeout when a working L2 but is also
        // waiting for crashed L1
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                receiverTimedOut();
            }
        }, 10000);
    }

    private void stopTimeout() {
        this.timer.cancel();
        this.timer = null;
        this.currentMessage = null;
        this.currentMessageReceiver = null;
    }

    private void receiverTimedOut() {
        // just use another L2 cache
        if (this.currentMessage != null && this.currentMessageReceiver != null) {
            // resend message
            System.out.printf("%s resend message to another L2\n", this.id);
            this.currentMessageReceiver.tell(this.currentMessage, this.getSelf());
            this.startTimeout();
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
        if (this.canInstantiateNewConversation()) {
            System.out.printf("%s can't instantiate new conversation, because it is waiting for response\n", this.id);
            return;
        }

        ActorRef l2Cache = message.getL2Cache();
        if (!this.l2Caches.contains(l2Cache)) {
            System.out.printf("%s The given L2 Cache is unknown\n", this.id);
            return;
        }
        System.out.printf("%s sends write message to L2 cache\n", this.id);

        // todo add l2 cache to some queue

        WriteMessage writeMessage = new WriteMessage(message.getKey(), message.getValue());
        l2Cache.tell(writeMessage, this.getSelf());
        this.currentMessage = writeMessage;
        this.currentMessageReceiver = l2Cache;
        this.startTimeout();
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
        if (this.canInstantiateNewConversation()) {
            System.out.printf("%s can't instantiate new conversation, because it is waiting for response\n", this.id);
            return;
        }

        ActorRef l2Cache = message.getL2Cache();
        if (!this.l2Caches.contains(l2Cache)) {
            System.out.printf("%s The given L2 Cache is unknown\n", this.id);
            return;
        }
        int key = message.getKey();
        System.out.printf("%s send read message to L2 Cache for key %d\n", this.id, key);

        ReadMessage readMessage = new ReadMessage(key, this.data.getUpdateCountForKey(key).orElse(0));
        this.currentMessage = readMessage;
        this.currentMessageReceiver = l2Cache;

        l2Cache.tell(readMessage, this.getSelf());
        this.startTimeout();
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
