package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.ReadConfig;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.WriteConfig;
import it.unitn.disi.ds1.multi_level_cache.messages.TimeoutMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;

public abstract class Node extends AbstractActor {

    private WriteConfig writeConfig = new WriteConfig();
    private ReadConfig readConfig = new ReadConfig();
    protected DataStore data = new DataStore();
    /** The timeout duration */
    static final long TIMEOUT_SECONDS = 6; // todo make milliseconds
    /** Data the Node knows about */
    /** ID of this node */
    public String id;

    public Node(String id) {
        super();
        this.id = id;
    }

    protected boolean isKeyLocked(int key) {
        return this.data.isLocked(key);
    }

    protected boolean isKeyAvailable(int key) {
        return this.data.containsKey(key);
    }

    protected boolean isWriteUnconfirmed(int key) {
        return this.writeConfig.isWriteUnconfirmed(key);
    }

    protected void addUnconfirmedWrite(int key, ActorRef actor) {
        this.writeConfig.addUnconfirmedWrite(key, actor);
    }

    protected void removeUnconfirmedWrite(int key) {
        this.writeConfig.removeUnconfirmedWrite(key);
    }

    protected boolean isReadUnconfirmed(int key) {
        return this.readConfig.isReadUnconfirmed(key);
    }

    protected void addUnconfirmedRead(int key, ActorRef actor) {
        this.readConfig.addUnconfirmedRead(key, actor);
    }

    protected void removeUnconfirmedRead(int key) {
        this.readConfig.removeUnconfirmedRead(key);
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

    protected void send(Serializable message, ActorRef receiver) {
        receiver.tell(message, this.getSelf());
    }

    /**
     * Returns the seconds used for a time-out.
     *
     * @return Seconds
     */
    protected long getTimeoutSeconds() {
        return TIMEOUT_SECONDS;
    }

    protected void scheduleMessageToSelf(Serializable message, long millis) {
        this.getContext()
                .system()
                .scheduler()
                .scheduleOnce(
                        Duration.ofMillis(millis),
                        this.getSelf(),
                        message,
                        this.getContext().system().dispatcher(),
                        this.getSelf()
                );
    }

    /**
     * Sends a TimeoutMessage to itself, after the given duration.
     */
    protected void setTimeout(Serializable message, ActorRef receiver, MessageType messageType) {
        TimeoutMessage timeoutMessage = new TimeoutMessage(message, receiver, messageType);
        this.scheduleMessageToSelf(timeoutMessage, this.getTimeoutSeconds());
    }

    protected void setMulticastTimeout(Serializable message, MessageType messageType) {
        TimeoutMessage timeoutMessage = new TimeoutMessage(message, ActorRef.noSender(), messageType);
        this.scheduleMessageToSelf(timeoutMessage, this.getTimeoutSeconds());
    }

    @Override
    public Receive createReceive() {
        return this.receiveBuilder().build();
    }

}
