package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.CrashMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.RecoveryMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.TimeoutMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Node extends AbstractActor {

    /** The timeout duration */
    static final long TIMEOUT_SECONDS = 2;
    /** Determines if this Node has crashed */
    protected boolean hasCrashed = false;
    /** Is this Node waiting for a write-confirm message */
    protected boolean isWaitingForWriteConfirm = false;
    /** WriteMessage retry count */
    protected int writeRetryCount = 0;
    /**
     * All unconfirmed read operations for the given key.
     * The value is the count of retries.
     */
    private Map<Integer, Integer> unconfirmedReads = new HashMap<>();
    /** ID of this node */
    public String id;

    public Node(String id) {
        super();
        this.id = id;
    }

    /**
     * Creates a Receive instance for when this Node has crashed.
     * THen, this Node will only handle RecoveryMessages.
     *
     * @return a Receive for crashed nodes
     */
    private Receive createReceiveForCrash() {
        return this.receiveBuilder()
                .match(RecoveryMessage.class, this::onRecovery)
                .build();
    }

    private void scheduleMessageToSelf(Serializable message, long seconds) {
        this.getContext()
                .system()
                .scheduler()
                .scheduleOnce(
                        Duration.ofSeconds(seconds),
                        this.getSelf(),
                        message,
                        this.getContext().system().dispatcher(),
                        this.getSelf()
                );
    }

    /**
     * Listener that is triggered whenever this node receives
     * a RecoveryMessage. Then, this node recovers from a crash.
     *
     * @param message The received RecoveryMessage.
     */
    private void onRecovery(RecoveryMessage message) {
        this.recover();
    }

    /**
     * Sends a message to all actors in the given group.
     *
     * @param message The message to be sent
     * @param group The receiving group of actors
     */
    protected void multicast(Serializable message, List<ActorRef> group) {
        for (ActorRef actor: group) {
            actor.tell(message, this.getSelf());
        }
    }

    /**
     * Returns the number of unconfirmed read operations.
     *
     * @return Number of unconfirmed read operations.
     */
    protected int getCurrentReadCount() {
        return this.unconfirmedReads.size();
    }

    /**
     * Returns the seconds used for a time-out.
     *
     * @return Seconds
     */
    protected long getTimeoutSeconds() {
        return TIMEOUT_SECONDS;
    }

    /**
     * Determines if this actor is allowed to instantiate a new conversation
     * for write. The rule is, an actor either allowed to handle a single write message
     * or multiple read messages at the same time. Therefore, no write or any read
     * conversation is allowed.
     *
     * @return Boolean that state if a write conversation is allowed
     */
    protected boolean canInstantiateNewWriteConversation() {
        return !this.hasCrashed && !this.isWaitingForWriteConfirm && this.getCurrentReadCount() <= 0;
    }

    /**
     * Determines if this actor is allowed to instantiate a new conversation
     * for read. The rule is, an actor either allowed to handle a single write message
     * or multiple read messages at the same time. Therefore, no current write conversation
     * is allowed.
     *
     * @return Boolean that state if a read conversation is allowed
     */
    protected boolean canInstantiateNewReadConversation() {
        return !this.hasCrashed && !this.isWaitingForWriteConfirm;
    }

    /**
     * Sends a TimeoutMessage to itself, after the given duration.
     */
    protected void setTimeout(Serializable message, ActorRef receiver, TimeoutType timeoutType) {
        TimeoutMessage timeoutMessage = new TimeoutMessage(message, receiver, timeoutType);
        this.scheduleMessageToSelf(timeoutMessage, this.getTimeoutSeconds());
    }

    /**
     * Adds the key to the unconfirmed read list.
     *
     * @param key Key of the read message
     */
    protected void addUnconfirmedReadMessage(int key) {
        if (!this.unconfirmedReads.containsKey(key)) {
            this.unconfirmedReads.put(key, 0);
        }
    }

    /**
     * Determines if an unconfirmed read message for the given key has been sent.
     *
     * @param key Key of the read
     * @return Boolean
     */
    protected boolean isReadUnconfirmed(int key) {
        return this.unconfirmedReads.containsKey(key);
    }

    /**
     * Returns the number of read retries for the given key.
     * 0 as default value.
     *
     * @param key Key of the read
     * @return Retry count
     */
    protected int getRetryCountForRead(int key) {
        return this.unconfirmedReads.getOrDefault(key, 0);
    }

    /**
     * Increases the read count for the given key by one.
     *
     * @param key Key of the read message
     */
    protected void increaseCountForUnconfirmedReadMessage(int key) {
        if (this.unconfirmedReads.containsKey(key)) {
            int retryCount = this.unconfirmedReads.getOrDefault(key, 0) + 1;
            this.unconfirmedReads.put(key, retryCount);
        }
    }

    /**
     * Removed the key from the unconfirmed read list.
     *
     * @param key Key of the read message
     */
    protected void resetUnconfirmedReadMessage(int key) {
        if (this.unconfirmedReads.containsKey(key)) {
            this.unconfirmedReads.remove(key);
        }
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
    }

    /**
     * Flushes all temp data
     */
    protected void flush() {
        this.unconfirmedReads = new HashMap<>();
        this.isWaitingForWriteConfirm = false;
    }

    /**
     * Recovers this node after it has crashed.
     */
    protected void recover() {
        if (this.hasCrashed) {
            System.out.printf("%s - Recovered\n", this.id);
            this.hasCrashed = false;
            this.getContext().become(this.createReceive());
        }
    }

    /**
     * Listener that is triggered whenever a Node receives a
     * CrashMessage.
     *
     * @param message The received CrashMessage
     */
    protected void onCrashMessage(CrashMessage message) {
        System.out.printf("%s - Crash\n", this.id);
        this.crash(message.getRecoverAfterSeconds());
    }

    protected abstract void onTimeoutMessage(TimeoutMessage message);

    @Override
    public Receive createReceive() {
        return this.receiveBuilder().build();
    }

}
