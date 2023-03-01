package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class Client extends AbstractActor {

    /** List of level 2 caches, the client knows about */
    private List<ActorRef> l2Caches;
    private DataStore data = new DataStore();
    private Timer timer;
    final String id;

    public Client(String id) {
        this.id = id;
    }

    static public Props props(String id) {
        return Props.create(Client.class, () -> new Client(id));
    }

    private void startTimeout() {
        // todo startTimeout(message)
        this.timer = new Timer();
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("4 SEK SPAETER");
            }
        }, 4000);
    }

    private void stopTimeout() {
        this.timer.cancel();
        this.timer = null;
    }

    private void receiverTimedOut() {
        // just use another L2 cache
    }

    private void onJoinL2Caches(JoinL2CachesMessage message) {
        this.l2Caches = List.copyOf(message.getL2Caches());
        System.out.printf("%s joined group of %d L2 caches\n", this.id, this.l2Caches.size());
    }

    private void onInstantiateWriteMessage(InstantiateWriteMessage message) {
        System.out.printf("%s sends write message to L2 cache\n", this.id);
        WriteMessage writeMessage = new WriteMessage(message.getKey(), message.getValue());
        message.getL2Cache().tell(writeMessage, this.getSelf());
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
        System.out.printf("%s send read message to L2 Cache\n", this.id);
        int key = message.getKey();
        ReadMessage readMessage = new ReadMessage(key, this.data.getUpdateCountForKey(key).orElse(0));
        message.getL2Cache().tell(readMessage, this.getSelf());
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
