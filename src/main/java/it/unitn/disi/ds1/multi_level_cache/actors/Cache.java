package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.io.Serializable;
import java.util.*;

public class Cache extends Node {

    /** A direct reference to the database */
    private ActorRef database;
    /**
     * A reference to the main L1 cache if this is a L2 cache.
     * Otherwise, null.
     */
    private ActorRef mainL1Cache;
    /** Contains all L1 caches except the one defined in mainL1Cache */
    private List<ActorRef> l1Caches;
    /**
     * A collection if all underlying L2 caches.
     * Null if this cache is a L2 cache.
     */
    private List<ActorRef> l2Caches;
    /** The write-message this cache is currently handling */
    private Optional<Serializable> currentWriteMessage = Optional.empty(); // todo still needed
    /** The sender of currentWriteMessage */
    private Optional<ActorRef> currentWriteSender = Optional.empty();  // todo still needed
    /**
     * Whenever an actor sends a read message to this cache, the actor is saved to this map
     * so the cache can directly reply.
     * */
    private Map<Serializable, ActorRef> currentReadMessages = new HashMap<>();  // todo still needed
    /**
     * Determines if this is the last level cache.
     * If true, this is the cache the clients talk to.
     */
    private final boolean isLastLevelCache;
    private Map<Integer, ActorRef> fillQueue = new HashMap<>(); // todo check if still necessary
    /** The cached data */
    private DataStore data = new DataStore();

    static public Props props(String id, boolean isLastLevelCache) {
        return Props.create(Cache.class, () -> new Cache(id, isLastLevelCache));
    }

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

    private void forwardMessageToNext(Serializable message) {
        if (this.isLastLevelCache) {
            this.mainL1Cache.tell(message, this.getSelf());
            this.setTimeout(message, this.mainL1Cache);
        } else {
            // no need for timeout, database can't crash
            this.database.tell(message, this.getSelf());
        }
    }

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
    private void multicast(Serializable message, List<ActorRef> group) {
        for (ActorRef actor: group) {
            actor.tell(message, this.getSelf());
        }
        // todo think about timeout
    }

    /**
     * Crashes this actor. It also resets all data except the knowledge
     * of other actors that are necessary.
     */
    @Override
    protected void crash() {
        super.crash();
        this.data.resetData();
        this.currentReadMessages = new HashMap<>();
        this.currentWriteMessage = Optional.empty();
        this.currentWriteSender = Optional.empty();
        this.fillQueue = new HashMap<>();
    }

    @Override
    protected void onTimeout(TimeoutMessage message) {
        if (this.isLastLevelCache) {
            System.out.printf("%s - has timed out, forward message directly to DB\n");
            this.database.tell(message, this.getSelf());
        }
    }

    /**
     * Sends a ReadReply message to the saved sender. A ReadReply message is only send
     * back to the client. Therefore, no need to start a timeout, since a client is
     * not supposed to crash.
     *
     * @param key The key received by the ReadMessage
     */
    private void responseReadReplyMessage(int key) {
        Optional<Integer> value = this.data.getValueForKey(key);
        Optional<Integer> updateCount = this.data.getUpdateCountForKey(key);

        if (this.currentReadMessages.containsKey(key) && value.isPresent() && updateCount.isPresent()) {
            // get client
            ActorRef client = this.currentReadMessages.get(key);
            this.currentReadMessages.remove(key);
            // send message
            ReadReplyMessage readReplyMessage = new ReadReplyMessage(key, value.get(), updateCount.get());
            client.tell(readReplyMessage, this.getSelf());
        }
    }

    private void responseFillMessage(int key) {
        // todo here we need to check if l2 has crashed, then read reply directly back to client (need to add client to the msg)
        Optional<Integer> value = this.data.getValueForKey(key);
        Optional<Integer> updateCount = this.data.getUpdateCountForKey(key);

        if (this.fillQueue.containsKey(key) && value.isPresent() && updateCount.isPresent()) {
            ActorRef cache = this.fillQueue.get(key);
            FillMessage fillMessage = new FillMessage(key, value.get(), updateCount.get());
            cache.tell(fillMessage, this.getSelf());
            this.fillQueue.remove(key);
        }
    }

    private void sendReFillMessage(int key, int value, int updateCount) {

    }

    private void responseForFillOrReadReply(int key) {
        if (!this.isLastLevelCache) {
            // forward fill to l2 cache
            this.responseFillMessage(key);
        } else {
            // answer read reply to client
            this.responseReadReplyMessage(key);
        }
    }

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
            RefillMessage reFillMessage = new RefillMessage(key, message.getValue(), message.getUpdateCount());
            for (ActorRef cache: this.l2Caches) {
                if (cache != sender) {
                    cache.tell(reFillMessage, this.getSelf());
                }
            }
        }
    }

    private void onRefillMessage(RefillMessage message) {
        if (this.hasCrashed) {
            // Can't do anything
            return;
        }

        int key = message.getKey();
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
            this.multicast(message, this.l2Caches);
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
        if (!this.isLastLevelCache) {
            // add l2 cache to fill queue
            this.fillQueue.put(key, this.getSender());
        } else {
            // add to read reply queue, only for last level cache, so we can reply to client
            this.currentReadMessages.put(key, this.getSender());
        }

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
