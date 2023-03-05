package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.io.Serializable;
import java.util.*;

public abstract class Cache extends Node {

    /** A direct reference to the database */
    protected ActorRef database;
    /**
     * A reference to the main L1 cache if this is a L2 cache.
     * Otherwise, null.
     */
    protected ActorRef mainL1Cache;
    /** Contains all L1 caches except the one defined in mainL1Cache */
    private List<ActorRef> l1Caches; // todo still needed?
    /**
     * A collection if all underlying L2 caches.
     * Null if this cache is a L2 cache.
     */
    protected List<ActorRef> l2Caches;
    /** The write-message this cache is currently handling */
    protected Optional<Serializable> currentWriteMessage = Optional.empty();
    /** The sender of currentWriteMessage */
    protected Optional<ActorRef> currentWriteSender = Optional.empty();
    /**
     * Whenever an actor sends a read message to this cache, the actor is saved to this map
     * so the cache can directly reply.
     * */
    protected Map<Integer, ActorRef> currentReadMessages = new HashMap<>(); // todo rename to current read sender
    /**
     * Determines if this is the last level cache.
     * If true, this is the cache the clients talk to.
     */
    protected final boolean isLastLevelCache;
    /** The cached data */
    protected DataStore data = new DataStore();

    public Cache(String id, boolean isLastLevelCache) {
        super(id);
        this.isLastLevelCache = isLastLevelCache;
    }

    /**
     * Updates the cache value for the given key if needed. The requirements are,
     * that the key already exists and the received value is newer. This method
     * should be called, whenever a ReFillMessage has been received.
     *
     * @param key Key of the value
     * @param value The new value
     * @param updateCount The update count of the received value
     */
    private void updateDataIfContained(int key, int value, int updateCount) {
        if (this.data.containsKey(key) && !this.data.isNewerOrEqual(key, updateCount)) {
            int currentUpdateCount = this.data.getUpdateCountForKey(key).orElse(0);
            System.out.printf("%s Update value: {%d :%d} (given UC: %d, my UC: %d)\n",
                    this.id, key, value, updateCount, currentUpdateCount);
            this.data.setValueForKey(key, value, currentUpdateCount);
        }
    }

    /**
     * Returns a boolean that states if this cache is allowed to handle another message.
     * The rule is; an actor can either handle a single write message or multiple read messages.
     * @return
     */
    private boolean canHandleReadMessage() {
        return this.currentWriteMessage.isEmpty();
    }

    private boolean canHandleWriteMessage() {
        return this.currentWriteMessage.isEmpty() && this.currentReadMessages.isEmpty();
    }

    abstract protected void forwardMessageToNext(Serializable message);

    private void forwardReadMessageToNext(Serializable message) {
        this.forwardMessageToNext(message);
        this.hasReceivedReadReply = false;
    }

    private void forwardWriteMessageToNext(Serializable message) {
        this.forwardMessageToNext(message);
        this.hasReceivedWriteConfirm = false;
    }

    /**
     * Sends a message to all actors in the given group.
     *
     * @param message
     * @param group
     */
    protected void multicast(Serializable message, List<ActorRef> group) {
        for (ActorRef actor: group) {
            actor.tell(message, this.getSelf());
        }
        // todo think about timeout
    }

    abstract protected void multicastReFillMessage(int key, int value, int updateCount, ActorRef sender);

    abstract protected void responseForFillOrReadReply(int key);

    protected void onJoinDatabase(JoinDatabaseMessage message) {
        this.database = message.getDatabase();
        System.out.printf("%s joined database\n", this.id);
    }

    protected void onJoinMainL1Cache(JoinMainL1CacheMessage message) {
        this.mainL1Cache = message.getL1Cache();
        System.out.printf("%s joined main L1 cache\n", this.id);
    }

    private void onJoinL1Caches(JoinL1CachesMessage message) {
        this.l1Caches = List.copyOf(message.getL1Caches());
        System.out.printf("%s joined group of %d L1 caches\n", this.id, this.l1Caches.size());
    }

    private void onJoinL2Caches(JoinL2CachesMessage message) {
        this.l2Caches = List.copyOf(message.getL2Caches());
        System.out.printf("%s joined group of %d L2 caches\n", this.id, this.l2Caches.size());
    }

    /**
     * Listener whenever this actor receives a WriteMessage. Then, this cache
     * is supposed to forward the message to the next actor. For L1 caches, this is
     * the DB, for L2 caches, the L1 cache.
     *
     * @param message The received write-message
     */
    private void onWriteMessage(WriteMessage message) {
        if (this.hasCrashed && !this.canHandleWriteMessage()) {
            // Can't do anything
            return;
        }
        System.out.printf("%s received write message, forward to next\n", this.id);

        // Need to save the message and sender
        this.currentWriteMessage = Optional.of(message);
        this.currentWriteSender = Optional.of(this.getSender());
        this.forwardWriteMessageToNext(message);
    }

    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        if (this.hasCrashed) {
            // Can't do anything
            return;
        }
        if (this.currentWriteMessage.isEmpty() || this.currentWriteSender.isEmpty()) {
            // todo error, no write message known
            return;
        }
        this.hasReceivedWriteConfirm = true;

        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        System.out.printf("%s received write confirm message {%d: %d} (UC: %d), forward to sender\n",
                this.id, value, updateCount);

        // Update value if needed
        // todo Is this correct?
        this.updateDataIfContained(key, value, updateCount);

        // Response confirm to sender
        ActorRef sender = this.currentWriteSender.get();
        sender.tell(message, this.getSelf());

        // Send refill to all other L2 caches except the sender
        if (!this.isLastLevelCache) {
            this.multicastReFillMessage(key, value, updateCount, sender);
        }
    }

    private void onRefillMessage(RefillMessage message) {
        if (this.hasCrashed) {
            // Can't do anything
            return;
        }

        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        System.out.printf(
                "%s received refill message for key %d. Update if needed.\n",
                this.id, key);

        // 1. Update if needed
        if (this.data.containsKey(key) && !this.data.isNewerOrEqual(key, message.getUpdateCount())) {
            // we need to update
            System.out.printf("%s Update value: {%d :%d} (given UC: %d, my UC: %d)\n",
                    this.id, key, message.getValue(), message.getUpdateCount(), this.data.getUpdateCountForKey(key).get());
            this.data.setValueForKey(key, message.getValue(), message.getUpdateCount());
        } else {
            // never known this key, don't update
            System.out.printf("%s never read/write key %d, therefore no update\n", this.id, key);
        }

        // 2. Now forward to previous level Caches
        if (!this.isLastLevelCache) {
            System.out.printf("%s need to reply to L2 caches, count: %d\n", this.id, this.l2Caches.size());
            this.multicastReFillMessage(key, value, updateCount, ActorRef.noSender());
        }
    }

    private void onReadMessage(ReadMessage message) {
        if (this.hasCrashed || !this.canHandleReadMessage()) {
            // Not allowed to handle received message -> time out
            return;
        }
        int key = message.getKey();
        int updateCount = message.getUpdateCount();
        System.out.printf("%s Received read message for key %d (UC: %d)\n", this.id, key, updateCount);

        // add sender to queue
        this.currentReadMessages.put(key, this.getSender());

        Optional<Integer> wanted = this.data.getValueForKey(key);
        // check if we own a more recent or an equal value
        if (wanted.isPresent() && this.data.isNewerOrEqual(key, updateCount)) {
            System.out.printf("%s knows an equal or more recent value: {%d :%d} (given UC: %d, my UC: %d)\n",
                    this.id, key, wanted.get(), updateCount, this.data.getUpdateCountForKey(key).get());
            // response accordingly
            this.responseForFillOrReadReply(key);
        } else {
            // We either don't know the value or it's older, so forward message to next
            System.out.printf("%s does not know %d or has an older version (given UC: %d, my UC: %d), forward to next\n",
                    this.id, key, updateCount, this.data.getUpdateCountForKey(key).orElse(0));
            this.forwardReadMessageToNext(message);
        }
    }

    /**
     * A fill message is received after a read message has been sent.
     * @param message
     */
    private void onFillMessage(FillMessage message) {
        if (this.hasCrashed) {
            // Can't do anything
            return;
        }
        int key = message.getKey();

        this.hasReceivedReadReply = true;

        /* No need to check the received update count. At this point, the received value is at least equal
        to our known value. See onReadMessage why.
         */

        System.out.printf("%s received fill message for {%d: %d} (given UC: %d, my UC: %d)\n",
                this.id, message.getKey(), message.getValue(), message.getUpdateCount(), this.data.getUpdateCountForKey(key).orElse(0));
        // Update value
        this.data.setValueForKey(key, message.getValue(), message.getUpdateCount());
        // response accordingly
        this.responseForFillOrReadReply(key);
    }

    private void onCrashMessage(CrashMessage message) {
        this.crash();
        System.out.printf("%s has crashed\n", this.id);
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinDatabaseMessage.class, this::onJoinDatabase)
                .match(JoinMainL1CacheMessage.class, this::onJoinMainL1Cache)
                .match(JoinL1CachesMessage.class, this::onJoinL1Caches)
                .match(JoinL2CachesMessage.class, this::onJoinL2Caches)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .match(RefillMessage.class, this::onRefillMessage)
                .match(ReadMessage.class, this::onReadMessage)
                .match(FillMessage.class, this::onFillMessage)
                .match(CrashMessage.class, this::onCrashMessage)
                .build();
    }

}
