package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
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
    /**
     * All unconfirmed read operations for the given key.
     * The value is the count of retries.
     */
    private Map<Integer, Integer> unconfirmedReads = new HashMap<>();
    /** Data the Node knows about */
    protected DataStore data = new DataStore();
    /** ID of this node */
    public String id;

    public Node(String id) {
        super();
        this.id = id;
    }

    /**
     * Determines if this actor is allowed to instantiate a new conversation
     * for write. The rule is, an actor either allowed to handle a single write message
     * or multiple read messages at the same time. Therefore, no write or any read
     * conversation is allowed.
     *
     * @return Boolean that state if a write conversation is allowed
     */
    protected abstract boolean canInstantiateNewWriteConversation(int key);

    /**
     * Determines if this actor is allowed to instantiate a new conversation
     * for read. The rule is, an actor either allowed to handle a single write message
     * or multiple read messages at the same time. Therefore, no current write conversation
     * is allowed.
     *
     * @return Boolean that state if a read conversation is allowed
     */
    protected abstract boolean canInstantiateNewReadConversation(int key);

    protected abstract void onTimeoutMessage(TimeoutMessage message);

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
     * Returns the seconds used for a time-out.
     *
     * @return Seconds
     */
    protected long getTimeoutSeconds() {
        return TIMEOUT_SECONDS;
    }

    protected void scheduleMessageToSelf(Serializable message, long seconds) {
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
     * Sends a TimeoutMessage to itself, after the given duration.
     */
    protected void setTimeout(Serializable message, ActorRef receiver, TimeoutType timeoutType) {
        TimeoutMessage timeoutMessage = new TimeoutMessage(message, receiver, timeoutType);
        this.scheduleMessageToSelf(timeoutMessage, this.getTimeoutSeconds());
    }

    protected void setMulticastTimeout(Serializable message, TimeoutType timeoutType) {
        TimeoutMessage timeoutMessage = new TimeoutMessage(message, ActorRef.noSender(), timeoutType);
        this.scheduleMessageToSelf(timeoutMessage, this.getTimeoutSeconds());
    }

    @Override
    public Receive createReceive() {
        return this.receiveBuilder().build();
    }

}
