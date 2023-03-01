package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.util.List;

public class Client extends AbstractActor {

    /** List of level 2 caches, the client knows about */
    private List<ActorRef> l2Caches;
    private DataStore data = new DataStore();
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

    private void onInstantiateWriteMessage(InstantiateWriteMessage message) {
        System.out.printf("%s sends write message to L2 cache\n", this.id);
        WriteMessage writeMessage = new WriteMessage(message.getKey(), message.getValue());
        message.getL2Cache().tell(writeMessage, this.getSelf());
    }

    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        System.out.printf("%s received write confirm for key %d (new UC: %d, old UC: %d)\n",
                this.id, key, value, updateCount);
        // update value
        this.data.setValueForKey(key, value, updateCount);
    }

    private void onInstantiateReadMessage(InstantiateReadMessage message) {
        System.out.printf("%s send read message to L2 Cache\n", this.id);
        int key = message.getKey();
        ReadMessage readMessage = new ReadMessage(key, this.data.getUpdateCountForKey(key).orElse(0));
        message.getL2Cache().tell(readMessage, this.getSelf());
    }

    private void onReadReplyMessage(ReadReplyMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        System.out.printf("%s Received read reply {%d: %d} (new UC: %d, old UC: %d)\n",
                this.id, key, value, updateCount, this.data.getUpdateCountForKey(key).orElse(0));
        // update value
        this.data.setValueForKey(key, value, updateCount);
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinGroupMessage.class, this::onJoinL2Cache)
                .match(InstantiateWriteMessage.class, this::onInstantiateWriteMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .match(InstantiateReadMessage.class, this::onInstantiateReadMessage)
                .match(ReadReplyMessage.class, this::onReadReplyMessage)
                .build();
    }

}
