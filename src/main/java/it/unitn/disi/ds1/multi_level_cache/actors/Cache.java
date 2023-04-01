package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;

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

    public Cache(String id) {
        super(id);
    }

    private void makeSelfCrash(long crashAfter, long recoverAfter) {
        CrashMessage crashMessage = new CrashMessage(recoverAfter);
        this.scheduleMessageToSelf(crashMessage, crashAfter);
    }

    private void makeSelfCrashIfNeeded(CacheCrashConfig l1CrashConfig, CacheCrashConfig l2CrashConfig) {
        if (this.isL1Cache() && l1CrashConfig.mustCrash()) {
            this.makeSelfCrash(l1CrashConfig.getCrashDelayMillis(), l1CrashConfig.getRecoverDelayMillis());
        } else if (!this.isL1Cache() && l2CrashConfig.mustCrash()) {
            this.makeSelfCrash(l2CrashConfig.getCrashDelayMillis(), l2CrashConfig.getRecoverDelayMillis());
        }
    }

    protected abstract void handleFill(int key);

    protected abstract void forwardMessageToNext(Serializable message, MessageType messageType);

    protected abstract void handleWriteMessage(WriteMessage message);

    protected abstract void handleRefillMessage(RefillMessage message);

    protected abstract void handleTimeoutMessage(TimeoutMessage message);

    protected abstract void handleCritWriteMessage(CritWriteMessage message);

    protected abstract void handleCritWriteRequestMessage(CritWriteRequestMessage message, boolean isOk);

    protected abstract void handleCritWriteVoteMessage(CritWriteVoteMessage message);

    protected abstract void handleCritWriteAbortMessage(CritWriteAbortMessage message);

    protected abstract void handleCritWriteCommitMessage(CritWriteCommitMessage message);

    protected abstract void handleErrorMessage(ErrorMessage message);

    protected abstract boolean isCritWriteOk(int key);

    protected abstract void abortCritWrite(int key);

    protected abstract boolean isL1Cache();

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

    protected void abortWrite(int key) {
        this.removeUnconfirmedWrite(key);
        this.unlockKey(key);
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
        this.forwardMessageToNext(message, MessageType.READ);
    }

    private void forwardCritReadMessageToNext(Serializable message, int key) {
        this.forwardMessageToNext(message, MessageType.CRITICAL_READ);
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
        boolean isLocked = this.isKeyLocked(key);
        Logger.write(this.id, LoggerOperationType.RECEIVED, key, value, isLocked);

        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            this.getSender().tell(ErrorMessage.lockedKey(key, MessageType.WRITE), this.getSelf());
            return;
        }

        // make crash
        CacheCrashConfig l1CrashConfig = message.getL1CrashConfig();
        CacheCrashConfig l2CrashConfig = message.getL2CrashConfig();
        this.makeSelfCrashIfNeeded(l1CrashConfig, l2CrashConfig);

        // lock
        this.lockKey(key);
        // set as unconfirmed
        this.addUnconfirmedWrite(message.getKey(), this.getSender());
        // forward to next
        Logger.write(this.id, LoggerOperationType.SEND, key, value, isLocked);
        this.forwardMessageToNext(message, MessageType.WRITE);
    }

    private void onCritWriteMessage(CritWriteMessage message) {
        int key = message.getKey();
        Logger.criticalWrite(this.id, LoggerOperationType.RECEIVED, key, message.getValue(), this.isKeyLocked(key));

        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            this.getSender().tell(ErrorMessage.lockedKey(key, MessageType.CRITICAL_WRITE), this.getSelf());
            return;
        }

        // make crash
        CacheCrashConfig l1CrashConfig = message.getL1CrashConfig();
        CacheCrashConfig l2CrashConfig = message.getL2CrashConfig();
        this.makeSelfCrashIfNeeded(l1CrashConfig, l2CrashConfig);

        this.handleCritWriteMessage(message);
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

        Logger.criticalWriteCommit(this.id, LoggerOperationType.RECEIVED, key, value, this.getValueOrElse(key), updateCount,
                this.getUpdateCountOrElse(key));

        // unlock and update
        this.unlockKey(key);
        try {
            this.setValue(key, value, updateCount);
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
        boolean isLocked = this.isKeyLocked(key);
        boolean isUnconfirmed = this.isWriteUnconfirmed(key);
        /*
        Only update if either,
        1. the data is locked and write-operation is unconfirmed (then, this was the requested Cache by client/L2)
        2. the data is unlocked and the message uc is newer than the current one (always update a write from the DB)
         */
        int actorUpdateCount = this.getUpdateCountOrElse(key);
        boolean isMsgNewer = updateCount > actorUpdateCount;
        boolean isLockedAndUnconfirmed = isLocked && isUnconfirmed;
        boolean mustUpdate =  isLockedAndUnconfirmed || (!isLocked && isMsgNewer);

        Logger.refill(this.id, LoggerOperationType.RECEIVED, key, value, this.getValueOrElse(key),
                updateCount, actorUpdateCount, isLocked, isUnconfirmed, mustUpdate);

        if (mustUpdate) {
            if (isLockedAndUnconfirmed) {
                // only unlock if this is the requested cache
                this.unlockKey(key);
            }

            try {
                this.setValue(key, value, updateCount);
                this.handleRefillMessage(message);
            } catch (IllegalAccessException e) {
                // Do nothing, if the data is locked then we don't update since critical write has priority
            }
        }
    }

    private void onReadMessage(ReadMessage message) {
        int key = message.getKey();

        if (this.isKeyLocked(key)) {
            // Not allowed to handle received message -> time out
            Logger.error(this.id, MessageType.READ, key, true, "Can't read value, because it's locked");
            this.getSender().tell(ErrorMessage.lockedKey(key, MessageType.READ), this.getSelf());
            return;
        }

        // make crash
        CacheCrashConfig l1CrashConfig = message.getL1CrashConfig();
        CacheCrashConfig l2CrashConfig = message.getL2CrashConfig();
        this.makeSelfCrashIfNeeded(l1CrashConfig, l2CrashConfig);

        int updateCount = message.getUpdateCount();
        int actorUpdateCount = this.getUpdateCountOrElse(key);
        // only forward if the message update count is older, or we don't know the value
        boolean isLocked = this.isKeyLocked(key);
        boolean isOlder = updateCount > actorUpdateCount;
        boolean mustForward = isOlder || !this.isKeyAvailable(key);
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
        } else {
            // response accordingly
            this.handleFill(key);
        }

        // set read as unconfirmed
        this.addUnconfirmedRead(key, this.getSender());
    }

    private void onCritReadMessage(CritReadMessage message) {
        int key = message.getKey();

        if (this.isKeyLocked(key)) {
            // Not allowed to handle received message -> time out
            Logger.error(this.id, MessageType.CRITICAL_READ, key, true, "Can't read value, because it's locked");
            this.getSender().tell(ErrorMessage.lockedKey(key, MessageType.CRITICAL_READ), this.getSelf());
            return;
        }

        // make crash
        CacheCrashConfig l1CrashConfig = message.getL1CrashConfig();
        CacheCrashConfig l2CrashConfig = message.getL2CrashConfig();
        this.makeSelfCrashIfNeeded(l1CrashConfig, l2CrashConfig);

        // add as unconfirmed
        this.addUnconfirmedRead(key, this.getSender());

        // print confirm
        int updateCount = message.getUpdateCount();
        Logger.criticalRead(this.id, LoggerOperationType.RECEIVED, key, updateCount, this.getUpdateCountOrElse(key),
                this.isKeyLocked(key));

        // Forward to next
        Logger.criticalRead(this.id, LoggerOperationType.SEND, key, updateCount, 0, this.isKeyLocked(key));
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
        Logger.fill(this.id, LoggerOperationType.RECEIVED, key, value, this.getValueOrElse(key), updateCount,
                this.getUpdateCountOrElse(key));

        // Update value
        try {
            this.setValue(key, message.getValue(), message.getUpdateCount());
            this.handleFill(key);
            // reset
            this.removeUnconfirmedRead(key);
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

    private void onErrorMessage(ErrorMessage message) {
        MessageType messageType = message.getMessageType();
        int key = message.getKey();

        if (messageType == MessageType.WRITE && this.isWriteUnconfirmed(key)) {
            // tell L2 about message
            ActorRef l2Cache = this.getUnconfirmedActorForWrit(key);
            l2Cache.tell(message, this.getSelf());
            // reset
            this.abortWrite(key);
        } else if (messageType == MessageType.CRITICAL_WRITE && !this.isCritWriteRequestConfirmed) {
            // tell L2 about message
            ActorRef l2Cache = this.unconfirmedWrites.get(key);
            l2Cache.tell(message, this.getSelf());
            // reset and just timeout
            this.resetCritWriteConfig(key);
            this.isCritWriteRequestConfirmed = false;
        } else if ((messageType == MessageType.READ || messageType == MessageType.CRITICAL_READ) && this.isReadUnconfirmed(key)) {

            // tell L2 about message
            List<ActorRef> l2Caches = this.getUnconfirmedActorsForRead(key);
            this.multicast(message, l2Caches);
            // reset
            this.removeUnconfirmedRead(key);
        }

        Logger.error(this.id, messageType, key, false, "Received error message");
        this.handleErrorMessage(message);
        // propagate the error message back until it reaches the conversation initiator

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
                .match(ErrorMessage.class, this::onErrorMessage)
                .build();
    }

}
