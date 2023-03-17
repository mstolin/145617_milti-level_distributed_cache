package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.messages.TimeoutMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;

public abstract class Node extends AbstractActor {

    /** The timeout duration */
    static final long TIMEOUT_SECONDS = 2;
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

    /**
     * Adds the key to the unconfirmed read list.
     *
     * @param key Key of the read message
     */
    protected abstract void addUnconfirmedReadMessage(int key, ActorRef sender);

    /**
     * Determines if an unconfirmed read message for the given key has been sent.
     *
     * @param key Key of the read
     * @return Boolean
     */
    protected abstract boolean isReadUnconfirmed(int key);

    /**
     * Removed the key from the unconfirmed read list.
     *
     * @param key Key of the read message
     */
    protected abstract void resetReadConfig(int key);

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
