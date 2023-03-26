package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerType;

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

    private void makeSelfCrash(long crashAfter, long recoverAfter) {
        CrashMessage crashMessage = new CrashMessage(recoverAfter);
        this.scheduleMessageToSelf(crashMessage, crashAfter);
    }

    private void makeSelfCrashOnReceivedIfNeeded(CacheCrashConfig l1CrashConfig, CacheCrashConfig l2CrashConfig) {
        if (this.isL1Cache() && l1CrashConfig.isCrashOnReceive()) {
            this.makeSelfCrash(l1CrashConfig.getCrashAfterOnReceive(), l1CrashConfig.getRecoverAfterOnReceive());
        } else if (!this.isL1Cache() && l2CrashConfig.isCrashOnReceive()) {
            this.makeSelfCrash(l2CrashConfig.getCrashAfterOnReceive(), l2CrashConfig.getRecoverAfterOnReceive());
        }
    }

    private void makeSelfCrashOnProcessedIfNeeded(CacheCrashConfig l1CrashConfig, CacheCrashConfig l2CrashConfig) {
        if (this.isL1Cache() && l1CrashConfig.isCrashOnProcessed()) {
            this.makeSelfCrash(l1CrashConfig.getCrashAfterOnProcessed(), l1CrashConfig.getRecoverAfterOnProcessed());
        } else if (!this.isL1Cache() && l2CrashConfig.isCrashOnProcessed()) {
            this.makeSelfCrash(l2CrashConfig.getCrashAfterOnProcessed(), l2CrashConfig.getRecoverAfterOnProcessed());
        }
    }

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

    protected abstract boolean isCritWriteOk(int key);

    protected abstract boolean isL1Cache();

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
            this.scheduleMessageToSelf(new RecoveryMessage(), recoverDelay);
        }
    }

    /**
     * Recovers this node after it has crashed.
     */
    protected void recover() {
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
        int value = message.getValue();
        boolean isLocked = this.data.isLocked(key);
        Logger.write(this.id, LoggerOperationType.RECEIVED, key, value, isLocked);

        if (!this.canInstantiateNewWriteConversation(key)) {
            // Can't do anything
            return;
        }

        // make crash
        CacheCrashConfig l1CrashConfig = message.getL1CrashConfig();
        CacheCrashConfig l2CrashConfig = message.getL2CrashConfig();
        this.makeSelfCrashOnReceivedIfNeeded(l1CrashConfig, l2CrashConfig);

        // lock
        this.data.lockValueForKey(key);
        // set as unconfirmed
        this.addUnconfirmedWrite(message.getKey(), this.getSender());
        // forward to next
        Logger.write(this.id, LoggerOperationType.SEND, key, value, isLocked);
        this.forwardMessageToNext(message, TimeoutType.WRITE);

        this.makeSelfCrashOnProcessedIfNeeded(l1CrashConfig, l2CrashConfig);
    }

    private void onCritWriteMessage(CritWriteMessage message) {
        int key = message.getKey();
        Logger.criticalWrite(this.id, LoggerOperationType.RECEIVED, key, message.getValue(), this.data.isLocked(key));

        if (!this.canInstantiateNewWriteConversation(key)) {
            // Can't do anything
            return;
        }

        // make crash
        CacheCrashConfig l1CrashConfig = message.getL1CrashConfig();
        CacheCrashConfig l2CrashConfig = message.getL2CrashConfig();
        this.makeSelfCrashOnReceivedIfNeeded(l1CrashConfig, l2CrashConfig);

        this.handleCritWriteMessage(message);

        this.makeSelfCrashOnProcessedIfNeeded(l1CrashConfig, l2CrashConfig);
    }

    private void onCritWriteRequestMessage(CritWriteRequestMessage message) {
        int key = message.getKey();
        boolean isOk = this.isCritWriteOk(key);
        Logger.criticalWriteRequest(this.id, LoggerOperationType.RECEIVED, key, isOk);
        this.handleCritWriteRequestMessage(message, isOk);
    }

    private void onCritWriteVoteMessage(CritWriteVoteMessage message) {
        this.handleCritWriteVoteMessage(message);
    }

    private void onCritWriteAbortMessage(CritWriteAbortMessage message) {
        Logger.criticalWriteAbort(this.id, LoggerOperationType.RECEIVED, message.getKey());
        this.handleCritWriteAbortMessage(message);
    }

    private void onCritWriteCommitMessage(CritWriteCommitMessage message) {
        // just update the value
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();

        Logger.criticalWriteCommit(this.id, LoggerOperationType.RECEIVED, key, value, this.data.getValueForKey(key).orElse(-1), updateCount,
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
        boolean isUnconfirmed = this.isWriteUnconfirmed(key);
        /*
        Only update if either,
        1. the data is locked and write-operation is unconfirmed (then, this was the requested Cache by client/L2)
        2. the data is unlocked and the message uc is newer than the current one (always update a write from the DB)
         */
        int actorUpdateCount = this.data.getUpdateCountForKey(key).orElse(0);
        boolean isMsgNewer = updateCount > actorUpdateCount;
        boolean isLockedAndUnconfirmed = isLocked && isUnconfirmed;
        boolean mustUpdate =  isLockedAndUnconfirmed || (!isLocked && isMsgNewer);

        Logger.refill(this.id, LoggerOperationType.RECEIVED, key, value, this.data.getValueForKey(key).orElse(-1),
                updateCount, actorUpdateCount, isLocked, isUnconfirmed, mustUpdate);

        if (mustUpdate) {
            if (isLockedAndUnconfirmed) {
                // only unlock if this is the requested cache
                this.data.unLockValueForKey(key);
            }

            try {
                this.data.setValueForKey(key, value, updateCount);
                this.handleRefillMessage(message);
            } catch (IllegalAccessException e) {
                // Do nothing, if the data is locked then we don't update since critical write has priority
            }
        }
    }

    private void onReadMessage(ReadMessage message) {
        int key = message.getKey();

        if (!this.canInstantiateNewReadConversation(key)) {
            // Not allowed to handle received message -> time out
            Logger.error(this.id, LoggerType.READ, key, true, "Can't read value, because it's locked");
            return;
        }

        // make crash
        CacheCrashConfig l1CrashConfig = message.getL1CrashConfig();
        CacheCrashConfig l2CrashConfig = message.getL2CrashConfig();
        this.makeSelfCrashOnReceivedIfNeeded(l1CrashConfig, l2CrashConfig);

        int updateCount = message.getUpdateCount();
        int actorUpdateCount = this.data.getUpdateCountForKey(key).orElse(0);
        // only forward if the message update count is older, or we don't know the value
        boolean isLocked = this.data.isLocked(key);
        boolean isOlder = updateCount > actorUpdateCount;
        boolean mustForward = isOlder || !this.data.containsKey(key);
        boolean isUnconfirmed = this.isReadUnconfirmed(key);
        Logger.read(this.id, LoggerOperationType.RECEIVED, key, updateCount, actorUpdateCount, isLocked, isOlder,
                isUnconfirmed);

        // check if we own a more recent or an equal value
        if (mustForward) {
            if (!isUnconfirmed) {
                // Maybe another client already requested to read this key, then only add as unconfirmed and wait for response
                Logger.read(this.id, LoggerOperationType.SEND, key, updateCount, 0, isLocked, isOlder,
                        false);
                this.forwardReadMessageToNext(message, key);
            }
            // add sender to queue
            this.addUnconfirmedReadMessage(key, this.getSender());
        } else {
            // response accordingly
            this.handleFill(key);
            // add sender to queue
            this.addUnconfirmedReadMessage(key, this.getSender());
        }

        this.makeSelfCrashOnProcessedIfNeeded(l1CrashConfig, l2CrashConfig);
    }

    private void onCritReadMessage(CritReadMessage message) {
        int key = message.getKey();

        if (!this.canInstantiateNewReadConversation(key)) {
            // Not allowed to handle received message -> time out
            Logger.error(this.id, LoggerType.CRITICAL_READ, key, true, "Can't read value, because it's locked");
            return;
        }

        // make crash
        CacheCrashConfig l1CrashConfig = message.getL1CrashConfig();
        CacheCrashConfig l2CrashConfig = message.getL2CrashConfig();
        this.makeSelfCrashOnReceivedIfNeeded(l1CrashConfig, l2CrashConfig);

        // add as unconfirmed
        this.addUnconfirmedReadMessage(key, this.getSender());

        // print confirm
        int updateCount = message.getUpdateCount();
        Logger.criticalRead(this.id, LoggerOperationType.RECEIVED, key, updateCount, this.data.getUpdateCountForKey(key).orElse(0),
                this.data.isLocked(key));

        // Forward to next
        Logger.criticalRead(this.id, LoggerOperationType.SEND, key, updateCount, 0, this.data.isLocked(key));
        this.forwardCritReadMessageToNext(message, key);

        this.makeSelfCrashOnProcessedIfNeeded(l1CrashConfig, l2CrashConfig);
    }

    /**
     * A fill message is received after a read message has been sent.
     * @param message
     */
    private void onFillMessage(FillMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        Logger.fill(this.id, LoggerOperationType.RECEIVED, key, value, this.data.getValueForKey(key).orElse(-1), updateCount,
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
        long recoverAfter = message.getRecoverAfter();
        Logger.crash(this.id, recoverAfter);
        this.recoverAfter(recoverAfter);
        this.flush();
    }

    /**
     * Listener that is triggered whenever this node receives
     * a RecoveryMessage. Then, this node recovers from a crash.
     *
     * @param message The received RecoveryMessage.
     */
    private void onRecoveryMessage(RecoveryMessage message) {
        Logger.recover(this.id, LoggerOperationType.RECEIVED);
        this.recover();
    }

    private void onTimeoutMessage(TimeoutMessage message) {
        this.handleTimeoutMessage(message);
    }

    private void onFlushMessage(FlushMessage message) {
        this.flush();
        Logger.flush(this.id, LoggerOperationType.RECEIVED);
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
