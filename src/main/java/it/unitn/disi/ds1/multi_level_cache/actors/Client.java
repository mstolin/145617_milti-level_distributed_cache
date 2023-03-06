package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

import java.util.*;

public class Client extends Node {

    /** List of level 2 caches, the client knows about */
    private List<ActorRef> l2Caches;
    /** Data the client knows about */
    private DataStore data = new DataStore();
    /** Is the client waiting for a write-message */
    private boolean hasSentWriteMessage = false;
    /** Number of read messages the client is waiting for */
    private int currentReadCount = 0;

    public Client(String id) {
        super(id);
    }

    static public Props props(String id) {
        return Props.create(Client.class, () -> new Client(id));
    }

    /**
     * Determines if this actor is allowed to instantiate a new conversation
     * for write. The rule is, an actor either allowed to handle a single write message
     * or multiple read messages at the same time. Therefore, no write or any read
     * conversation is allowed.
     *
     * @return Boolean that state if a write conversation is allowed
     */
    private boolean canInstantiateNewWriteConversation() {
        return !this.hasSentWriteMessage && this.currentReadCount <= 0;
    }

    /**
     * Determines if this actor is allowed to instantiate a new conversation
     * for read. The rule is, an actor either allowed to handle a single write message
     * or multiple read messages at the same time. Therefore, no current write conversation
     * is allowed.
     *
     * @return Boolean that state if a read conversation is allowed
     */
    private boolean canInstantiateNewReadConversation() {
        return !this.hasSentWriteMessage;
    }

    /**
     * Sends a WriteMessage instance to the given L2 cache.
     * It also starts a write-timeout.
     *
     * @param l2Cache The choosen L2 cache actor
     * @param key Key that has to be written
     * @param value Value used to update the key
     */
    private void tellWriteMessage(ActorRef l2Cache, int key, int value) {
        WriteMessage writeMessage = new WriteMessage(key, value);
        l2Cache.tell(writeMessage, this.getSelf());
        // set config
        this.hasReceivedWriteConfirm = false;
        this.hasSentWriteMessage = true;
        // set timeout
        this.setTimeout(writeMessage, l2Cache, TimeoutType.WRITE);
    }

    /**
     * Sends a ReadMessage to the given L2 cache. Additionally, it increases the read count
     * and start a timeout for the read message.
     *
     * @param l2Cache Target L2 cache
     * @param key Key to be read
     */
    private void tellReadMessage(ActorRef l2Cache, int key) {
        ReadMessage readMessage = new ReadMessage(key, this.data.getUpdateCountForKey(key).orElse(0));
        l2Cache.tell(readMessage, this.getSelf());
        // set config
        this.hasReceivedReadReply = false;
        this.currentReadCount = this.currentReadCount + 1;
        // set timeout
        this.setTimeout(readMessage, l2Cache, TimeoutType.READ);
    }

    /**
     * Event listener that is triggered when this actor receives a
     * JoinL2CachesMessage. Then it is supposed to join a group of L2
     * cache actor instances.
     *
     * @param message The received JoinL2CachesMessage
     */
    private void onJoinL2Caches(JoinL2CachesMessage message) {
        this.l2Caches = List.copyOf(message.getL2Caches());
        System.out.printf("%s joined group of %d L2 caches\n", this.id, this.l2Caches.size());
    }

    /**
     * Listener that is triggered whenever this actor receives a InstantiateWriteMessage.
     * Then, the actor is supposed to send a WriteMessage to the given L2 cache.
     *
     * @param message The received InstantiateWriteMessage
     */
    private void onInstantiateWriteMessage(InstantiateWriteMessage message) {
        int key = message.getKey();
        int value = message.getValue();

        if (!this.canInstantiateNewWriteConversation()) {
            System.out.printf("%s can't instantiate new write for {%d: %d}, because it is waiting for response\n",
                    this.id, key, value);
            return;
        }

        ActorRef l2Cache = message.getL2Cache();
        if (!this.l2Caches.contains(l2Cache)) {
            System.out.printf("%s The given L2 Cache is unknown\n", this.id);
            return;
        }

        System.out.printf("%s sends write message {%d: %d} to L2 cache\n", this.id, key, value);
        this.tellWriteMessage(l2Cache, key, value);
    }

    /**
     * Listener that is triggered whenever this actor receives a WriteConfirmMessage.
     * Then, a previous WriteMessage has been sent successfully, and this actor needs
     * to update its value and stop the write-timeout.
     *
     * @param message The received WriteConfirmMessage
     */
    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        System.out.printf("%s received write confirm for key %d: %d (new UC: %d, old UC: %d)\n",
                this.id, key, value, updateCount, this.data.getUpdateCountForKey(key).orElse(0));

        // update value
        this.data.setValueForKey(key, value, updateCount);

        // reset config
        this.hasReceivedWriteConfirm = true;
        this.hasSentWriteMessage = false;
    }

    /**
     * Listener that is triggered whenever this actor receives an InstantiateReadMessage. Then,
     * this actor is supposed to send a ReadMessage to the given L2 cache actor, start the timeout and
     * increase the read counter.
     *
     * @param message The received InstantiateReadMessage.
     */
    private void onInstantiateReadMessage(InstantiateReadMessage message) {
        int key = message.getKey();

        if (!this.canInstantiateNewReadConversation()) {
            System.out.printf("%s can't instantiate new read conversation for key %d, because it is waiting for response\n",
                    this.id, key);
            return;
        }

        ActorRef l2Cache = message.getL2Cache();
        if (!this.l2Caches.contains(l2Cache)) {
            System.out.printf("%s The given L2 Cache is unknown\n", this.id);
            return;
        }

        System.out.printf("%s send read message to L2 Cache for key %d\n", this.id, key);
        this.tellReadMessage(l2Cache, key);
    }

    /**
     * Event listener that is triggered whenever this actor receives a ReadReplyMessage
     * message. Then, a previous ReadMessage was sent successfully and this actor has to update
     * the value and stop the timer.
     *
     * @param message The received ReadMessage
     */
    private void onReadReplyMessage(ReadReplyMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        System.out.printf("%s Received read reply {%d: %d} (new UC: %d, old UC: %d)\n",
                this.id, key, value, updateCount, this.data.getUpdateCountForKey(key).orElse(0));

        // update value
        this.data.setValueForKey(key, value, updateCount);

        // reset config
        this.currentReadCount = this.currentReadCount - 1;
        this.hasReceivedReadReply = true;
    }

    @Override
    protected void onTimeoutMessage(TimeoutMessage message) {
        TimeoutType type = message.getType();
        if (type == TimeoutType.WRITE && !this.hasReceivedWriteConfirm) {
            WriteMessage writeMessage = (WriteMessage) message.getMessage();
            System.out.printf("%s - Timeout on WriteMessage for {%2d: %d}\n",
                    this.id, writeMessage.getKey(), writeMessage.getValue());
        } else if (type == TimeoutType.READ && !this.hasReceivedReadReply) {
            ReadMessage readMessage = (ReadMessage) message.getMessage();
            System.out.printf("%s - Timeout on ReadMessage for key %2d\n", this.id, readMessage.getKey());
        }
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
                .match(TimeoutMessage.class, this::onTimeoutMessage)
                .build();
    }

}
