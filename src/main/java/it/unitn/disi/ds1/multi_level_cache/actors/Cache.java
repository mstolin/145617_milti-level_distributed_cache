package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

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
    /**
     * A collection if all underlying L2 caches.
     * Null if this cache is a L2 cache.
     */
    protected List<ActorRef> l2Caches;
    /** Determines if this Node has crashed */
    protected boolean hasCrashed = false;
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
    protected Map<Integer, List<ActorRef>> unconfirmedReadSender = new HashMap<>();
    protected Map<Integer, List<ActorRef>> unconfirmedWriteSender = new HashMap<>();

    public Cache(String id, boolean isLastLevelCache) {
        super(id);
        this.isLastLevelCache = isLastLevelCache;
    }

    protected abstract void multicastReFillMessage(int key, int value, int updateCount, ActorRef sender);

    protected abstract void responseForFillOrReadReply(int key);

    protected abstract void forwardMessageToNext(Serializable message, TimeoutType timeoutType);

    protected abstract void onCritWriteRequestMessage(CritWriteRequestMessage message);

    protected abstract void onCritWriteVoteMessage(CritWriteVoteMessage message);

    protected abstract void onCritWriteAbortMessage(CritWriteAbortMessage message);

    protected abstract void onCritWriteCommitMessage(CritWriteCommitMessage message);

    /**
     * Flushes all temporary data.
     */
    protected void flush() {
        this.data.resetData();
        this.currentReadMessages = new HashMap<>();
        this.currentWriteMessage = Optional.empty();
        this.currentWriteSender = Optional.empty();
    }

    /**
     * Crashes this node
     */
    protected void crash(long recoverDelay) {
        this.hasCrashed = true;
        this.getContext().become(this.createReceiveForCrash());

        if (recoverDelay > 0) {
            System.out.printf("%s - Recover after %d\n", this.id, recoverDelay);
            this.scheduleMessageToSelf(new RecoveryMessage(), recoverDelay);
        }

        this.flush();
    }

    /**
     * Recovers this node after it has crashed.
     */
    protected void recover() {
        if (this.hasCrashed) {
            System.out.printf("%s - Recovered\n", this.id);
            this.hasCrashed = false;
            this.getContext().become(this.createReceive());
            this.data.unLockAll();
        }
    }

    @Override
    protected boolean canInstantiateNewReadConversation(int key) {
        return !this.data.isLocked(key);
    }

    @Override
    protected boolean canInstantiateNewWriteConversation(int key) {
        return !this.data.isLocked(key);
    }

    /**
     * Creates a Receive instance for when this Node has crashed.
     * THen, this Node will only handle RecoveryMessages.
     *
     * @return a Receive for crashed nodes
     */
    private Receive createReceiveForCrash() {
        return this.receiveBuilder()
                .match(RecoveryMessage.class, this::onRecoveryMessage)
                .build();
    }

    private void saveWriteConfig(Serializable message, ActorRef sender) {
        this.currentWriteMessage = Optional.of(message);
        this.currentWriteSender = Optional.of(sender);
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

    private void forwardReadMessageToNext(Serializable message, int key) {
        this.forwardMessageToNext(message, TimeoutType.READ);
        this.addUnconfirmedReadMessage(key);
    }

    private void forwardCritReadMessageToNext(Serializable message, int key) {
        this.forwardMessageToNext(message, TimeoutType.CRIT_READ);
        this.addUnconfirmedReadMessage(key);
    }

    private void forwardWriteMessageToNext(Serializable message) {
        this.forwardMessageToNext(message, TimeoutType.WRITE);
        this.isWaitingForWriteConfirm = true;
    }

    private void onJoinDatabase(JoinDatabaseMessage message) {
        this.database = message.getDatabase();
        System.out.printf("%s joined database\n", this.id);
    }

    private void onJoinMainL1Cache(JoinMainL1CacheMessage message) {
        this.mainL1Cache = message.getL1Cache();
        System.out.printf("%s joined main L1 cache\n", this.id);
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
        int key = message.getKey();

        if (!this.canInstantiateNewWriteConversation(key)) {
            // Can't do anything
            return;
        }
        System.out.printf("%s - Received write message, forward to next\n", this.id);

        // Need to save the message and sender
        this.saveWriteConfig(message, this.getSender());

        this.forwardWriteMessageToNext(message);
    }

    private void onCritWriteMessage(CritWriteMessage message) {
        int key = message.getKey();

        if (!this.canInstantiateNewWriteConversation(key)) {
            // Can't do anything
            return;
        }
        System.out.printf("%s - Received critical write message, forward to next\n", this.id);

        // block for write
        this.saveWriteConfig(message, this.getSender());
        // simply forward
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
        // Disable timout
        this.isWaitingForWriteConfirm = false;

        // print confirm
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        System.out.printf("%s received write confirm message {%d: %d} (UC: %d), forward to sender\n",
                this.id, key, value, updateCount);

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

        // Print confirm
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
        int key = message.getKey();

        if (!this.canInstantiateNewReadConversation(key)) {
            // Not allowed to handle received message -> time out
            return;
        }
        // print confirm
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
            this.forwardReadMessageToNext(message, key);
        }
    }

    private void onCritReadMessage(CritReadMessage message) {
        int key = message.getKey();

        if (!this.canInstantiateNewReadConversation(key)) {
            // Not allowed to handle received message -> time out
            return;
        }
        // print confirm
        int updateCount = message.getUpdateCount();
        System.out.printf("%s - Received crit read message for key %d (UC: %d)\n", this.id, key, updateCount);

        // add sender to queue
        this.currentReadMessages.put(key, this.getSender());

        // Forward to next
        this.forwardCritReadMessageToNext(message, key);
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
        System.out.printf("%s received fill message for {%d: %d} (given UC: %d, my UC: %d)\n",
                this.id, message.getKey(), message.getValue(), message.getUpdateCount(), this.data.getUpdateCountForKey(key).orElse(0));

        // disable timout
        this.resetUnconfirmedReadMessage(key);
        // todo resetReadConfig(key) like in client

        // Update value
        this.data.setValueForKey(key, message.getValue(), message.getUpdateCount());
        // response accordingly
        this.responseForFillOrReadReply(key);
    }

    /**
     * Listener that is triggered whenever a Node receives a
     * CrashMessage.
     *
     * @param message The received CrashMessage
     */
    private void onCrashMessage(CrashMessage message) {
        System.out.printf("%s - Crash\n", this.id);
        this.crash(message.getRecoverAfterSeconds());
    }

    /**
     * Listener that is triggered whenever this node receives
     * a RecoveryMessage. Then, this node recovers from a crash.
     *
     * @param message The received RecoveryMessage.
     */
    private void onRecoveryMessage(RecoveryMessage message) {
        this.recover();
    }

    private void onFlushMessage(FlushMessage message) {
        this.flush();
        System.out.printf("%s - Flushed\n", this.id);
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinDatabaseMessage.class, this::onJoinDatabase)
                .match(JoinMainL1CacheMessage.class, this::onJoinMainL1Cache)
                .match(JoinL2CachesMessage.class, this::onJoinL2Caches)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(CritWriteMessage.class, this::onCritWriteMessage)
                .match(CritWriteRequestMessage.class, this::onCritWriteRequestMessage)
                .match(CritWriteVoteMessage.class, this::onCritWriteVoteMessage)
                .match(CritWriteAbortMessage.class, this::onCritWriteAbortMessage)
                .match(CritWriteCommitMessage.class, this::onCritWriteCommitMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .match(RefillMessage.class, this::onRefillMessage)
                .match(ReadMessage.class, this::onReadMessage)
                .match(CritReadMessage.class, this::onCritReadMessage)
                .match(FillMessage.class, this::onFillMessage)
                .match(CrashMessage.class, this::onCrashMessage)
                .match(TimeoutMessage.class, this::onTimeoutMessage)
                .match(FlushMessage.class, this::onFlushMessage)
                .build();
    }

}
