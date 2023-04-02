package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.ReadConfig;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.WriteConfig;
import it.unitn.disi.ds1.multi_level_cache.messages.ErrorMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.TimeoutMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

public abstract class Node extends AbstractActor {

    private WriteConfig writeConfig = new WriteConfig();
    private ReadConfig readConfig = new ReadConfig();
    private DataStore data = new DataStore();
    /** The timeout duration */
    static final long TIMEOUT_SECONDS = 6; // todo make milliseconds
    /** Data the Node knows about */
    /** ID of this node */
    public String id;

    public Node(String id) {
        super();
        this.id = id;
    }

    protected abstract void handleErrorMessage(ErrorMessage message);

    protected abstract void handleTimeoutMessage(TimeoutMessage message);

    protected void lockKey(int key) {
        this.data.lockValueForKey(key);
    }

    protected void unlockKey(int key) {
        this.data.unLockValueForKey(key);
    }

    protected Optional<Integer> getValue(int key) {
        return this.data.getValueForKey(key);
    }

    protected int getValueOrElse(int key) {
        return this.data.getValueForKey(key).orElse(-1);
    }

    protected Optional<Integer> getUpdateCount(int key) {
        return this.data.getUpdateCountForKey(key);
    }

    protected int getUpdateCountOrElse(int key) {
        return this.data.getUpdateCountForKey(key).orElse(0);
    }

    protected void setValue(int key, int value) throws IllegalAccessException {
        this.data.setValueForKey(key, value);
    }

    protected void setValue(int key, int value, int updateCount) throws IllegalAccessException {
        this.data.setValueForKey(key, value, updateCount);
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

    protected ActorRef getUnconfirmedActorForWrit(int key) {
        return this.writeConfig.getUnconfirmedActor(key);
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

    protected List<ActorRef> getUnconfirmedActorsForRead(int key) {
        return this.readConfig.getUnconfirmedActors(key);
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

    protected void flush() {
        this.data = new DataStore();
        this.writeConfig = new WriteConfig();
        this.readConfig = new ReadConfig();
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

    protected void onErrorMessage(ErrorMessage message) {
        Logger.error(this.id, message.getMessageType(), message.getKey(), false, "Received error message");
        this.handleErrorMessage(message);
    }

    protected void onTimeoutMessage(TimeoutMessage message) {
        this.handleTimeoutMessage(message);
    }

    @Override
    public Receive createReceive() {
        return this.receiveBuilder().build();
    }

}
