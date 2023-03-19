package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger;

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
    protected Map<Integer, List<ActorRef>> unconfirmedReads = new HashMap<>();
    protected Map<Integer, ActorRef> unconfirmedWrites = new HashMap<>();

    public Cache(String id) {
        super(id);
    }

    protected abstract void multicastReFillMessageIfNeeded(int key, int value, int updateCount, ActorRef sender);

    protected abstract void handleFill(int key);

    protected abstract void forwardMessageToNext(Serializable message, TimeoutType timeoutType);

    protected abstract void handleWriteMessage(WriteMessage message);

    protected abstract void handleRefillMessage(RefillMessage message);

    protected abstract void handleTimeoutMessage(TimeoutMessage message);

    protected abstract void handleCritWriteMessage(CritWriteMessage message);

    protected abstract void handleCritWriteRequestMessage(CritWriteRequestMessage message, boolean isOk);

    protected abstract void handleCritWriteVoteMessage(CritWriteVoteMessage message);

    protected abstract void handleCritWriteAbortMessage(CritWriteAbortMessage message);

    protected abstract void handleCritWriteCommitMessage(CritWriteCommitMessage message);

    /**
     * Flushes all temporary data.
     */
    protected void flush() {
        this.data.resetData();
        this.unconfirmedReads = new HashMap<>();
        this.unconfirmedWrites = new HashMap<>();
    }

    /**
     * Crashes this node
     */
    protected void recoverAfter(long recoverDelay) {
        this.getContext().become(this.createReceiveForCrash());

        if (recoverDelay > 0) {
            System.out.printf("%s - Recover after %d\n", this.id, recoverDelay);
            this.scheduleMessageToSelf(new RecoveryMessage(), recoverDelay);
        }
    }

    /**
     * Recovers this node after it has crashed.
     */
    protected void recover() {
        System.out.printf("%s - Recovered\n", this.id);
        this.getContext().become(this.createReceive());
    }

    protected void addUnconfirmedWrite(int key, ActorRef sender) {
        if (!this.unconfirmedWrites.containsKey(key)) {
            this.unconfirmedWrites.put(key, sender);
        }
    }

    protected boolean isWriteUnconfirmed(int key) {
        return this.unconfirmedWrites.containsKey(key);
    }

    protected void resetWriteConfig(int key) { // todo rename abortWrite
        // remove from unconfirmed
        if (this.unconfirmedWrites.containsKey(key)) {
            this.unconfirmedWrites.remove(key);
        }
        // unlock
        this.data.unLockValueForKey(key);
    }

    /**
     * A Cache a cannot handle the read conversation if the value is locked.
     *
     * @param key
     * @return
     */
    @Override
    protected boolean canInstantiateNewReadConversation(int key) {
        return !this.data.isLocked(key);
    }

    /**
     * A Cache cannot handle a new write conversation, if the value is locked
     * or another actor has already requested to write this key.
     *
     * @param key
     * @return
     */
    @Override
    protected boolean canInstantiateNewWriteConversation(int key) {
        return !this.data.isLocked(key) && !this.isWriteUnconfirmed(key);
    }

    @Override
    protected void addUnconfirmedReadMessage(int key, ActorRef sender) {
        if (this.unconfirmedReads.containsKey(key)) {
            this.unconfirmedReads.get(key).add(sender);
        } else {
            List<ActorRef> actors = new ArrayList<>(List.of(sender));
            this.unconfirmedReads.put(key, actors);
        }
    }

    @Override
    protected boolean isReadUnconfirmed(int key) {
        return this.unconfirmedReads.containsKey(key);
    }

    @Override
    protected void resetReadConfig(int key) {
        if (this.unconfirmedReads.containsKey(key)) {
            this.unconfirmedReads.remove(key);
        }
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

    /**
     * Updates the cache value for the given key if needed. The requirements are,
     * that the key already exists and the received value is newer. This method
     * should be called, whenever a ReFillMessage has been received.
     *
     * @param key Key of the value
     * @param value The new value
     * @param updateCount The update count of the received value
     */
    private void updateDataIfContained(int key, int value, int updateCount) throws IllegalAccessException {
        if (this.data.containsKey(key) && !this.data.isNewerOrEqual(key, updateCount)) {
            int currentUpdateCount = this.data.getUpdateCountForKey(key).orElse(0);
            System.out.printf("%s Update value: {%d :%d} (given UC: %d, my UC: %d)\n",
                    this.id, key, value, updateCount, currentUpdateCount);
            this.data.setValueForKey(key, value, currentUpdateCount);
        }
    }

    private void forwardReadMessageToNext(Serializable message, int key) {
        this.forwardMessageToNext(message, TimeoutType.READ);
    }

    private void forwardCritReadMessageToNext(Serializable message, int key) {
        this.forwardMessageToNext(message, TimeoutType.CRIT_READ);
    }

    private void onJoinDatabase(JoinDatabaseMessage message) {
        this.database = message.getDatabase();
        Logger.join(this.id, "Database", 1);
    }

    private void onJoinMainL1Cache(JoinMainL1CacheMessage message) {
        this.mainL1Cache = message.getL1Cache();
        Logger.join(this.id, "L1 Cache", 1);
    }

    private void onJoinL2Caches(JoinL2CachesMessage message) {
        this.l2Caches = List.copyOf(message.getL2Caches());
        Logger.join(this.id, "L2 Caches", this.l2Caches.size());
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
        Logger.write(this.id, message.getKey(), message.getValue(), this.data.isLocked(key));

        if (!this.canInstantiateNewWriteConversation(key)) {
            // Can't do anything
            return;
        }

        this.data.lockValueForKey(key);

        this.handleWriteMessage(message);
    }

    private void onCritWriteMessage(CritWriteMessage message) {
        int key = message.getKey();
        Logger.criticalWrite(this.id, key, message.getValue(), this.data.isLocked(key));

        if (!this.canInstantiateNewWriteConversation(key)) {
            // Can't do anything
            return;
        }

        this.handleCritWriteMessage(message);
    }

    private void onCritWriteRequestMessage(CritWriteRequestMessage message) {
        int key = message.getKey();
        boolean isOk = !this.data.isLocked(key) && !this.isWriteUnconfirmed(key);
        Logger.criticalWriteRequest(this.id, key, isOk);
        this.handleCritWriteRequestMessage(message, isOk);
    }

    private void onCritWriteVoteMessage(CritWriteVoteMessage message) {
        this.handleCritWriteVoteMessage(message);
    }

    private void onCritWriteAbortMessage(CritWriteAbortMessage message) {
        Logger.criticalWriteAbort(this.id);
        this.handleCritWriteAbortMessage(message);
    }

    private void onCritWriteCommitMessage(CritWriteCommitMessage message) {
        // just update the value
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();

        Logger.criticalWriteCommit(this.id, key, value, this.data.getValueForKey(key).orElse(-1), updateCount,
                this.data.getUpdateCountForKey(key).orElse(0));

        // unlock and update
        this.data.unLockValueForKey(key);
        try {
            this.data.setValueForKey(key, value, updateCount);
            this.handleCritWriteCommitMessage(message);
        } catch (IllegalAccessException e) {
            // nothing going on here, value is unlocked anyway
        }
    }

    private void onRefillMessage(RefillMessage message) {
        // Print confirm
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        boolean isLocked = this.data.isLocked(key);
        boolean mustSave = this.data.containsKey(key) && !this.data.isNewerOrEqual(key, message.getUpdateCount());

        Logger.refill(this.id, key, value, this.data.getValueForKey(key).orElse(-1), updateCount,
                this.data.getUpdateCountForKey(key).orElse(0), isLocked, mustSave);

        // todo Must be saved if unconfirmed

        if (!isLocked && mustSave) {
            try {
                this.data.setValueForKey(key, value, updateCount);
            } catch (IllegalAccessException e) {
                // Do nothing, if the data is locked then we don't update since critical write has priority
            }
        }

        // todo Check if this has to get into try
        this.handleRefillMessage(message);
    }

    private void onReadMessage(ReadMessage message) {
        int key = message.getKey();

        if (!this.canInstantiateNewReadConversation(key)) {
            // Not allowed to handle received message -> time out
            return;
        }

        // add sender to queue
        this.addUnconfirmedReadMessage(key, this.getSender());

        int updateCount = message.getUpdateCount();
        boolean mustForward = !this.data.isNewerOrEqual(key, updateCount);
        Logger.read(this.id, key, updateCount, this.data.getUpdateCountForKey(key).orElse(0), mustForward);

        // check if we own a more recent or an equal value
        if (mustForward) {
            // We either don't know the value or it's older, so forward message to next
            this.forwardReadMessageToNext(message, key);
        } else {
            // response accordingly
            this.handleFill(key);
        }
    }

    private void onCritReadMessage(CritReadMessage message) {
        int key = message.getKey();

        if (!this.canInstantiateNewReadConversation(key)) {
            // Not allowed to handle received message -> time out
            return;
        }
        // add as unconfirmed
        this.addUnconfirmedReadMessage(key, this.getSender());

        // print confirm
        int updateCount = message.getUpdateCount();
        Logger.criticalRead(this.id, key, updateCount, this.data.getUpdateCountForKey(key).orElse(0));

        // Forward to next
        this.forwardCritReadMessageToNext(message, key);
    }

    /**
     * A fill message is received after a read message has been sent.
     * @param message
     */
    private void onFillMessage(FillMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        Logger.fill(this.id, key, value, this.data.getValueForKey(key).orElse(-1), updateCount,
                this.data.getUpdateCountForKey(key).orElse(0));

        // Update value
        try {
            this.data.setValueForKey(key, message.getValue(), message.getUpdateCount());
            this.handleFill(key);
            // reset
            this.resetReadConfig(key);
        } catch (IllegalAccessException e) {
            // Do nothing, critical write has higher priority, just timeout
        }
    }

    /**
     * Listener that is triggered whenever a Node receives a
     * CrashMessage.
     *
     * @param message The received CrashMessage
     */
    private void onCrashMessage(CrashMessage message) {
        Logger.crash(this.id);
        this.recoverAfter(message.getRecoverAfterSeconds());
        this.flush();
    }

    /**
     * Listener that is triggered whenever this node receives
     * a RecoveryMessage. Then, this node recovers from a crash.
     *
     * @param message The received RecoveryMessage.
     */
    private void onRecoveryMessage(RecoveryMessage message) {
        Logger.recover(this.id);
        this.recover();
    }

    private void onTimeoutMessage(TimeoutMessage message) {
        this.handleTimeoutMessage(message);
    }

    private void onFlushMessage(FlushMessage message) {
        this.flush();
        Logger.flush(this.id);
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
