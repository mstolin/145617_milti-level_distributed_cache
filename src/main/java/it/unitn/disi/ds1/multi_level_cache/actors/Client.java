package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageConfig;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Client extends Node {

    /**
     * The timeout duration in millis. It is important that the
     * time-out delay for the client is slightly longer than the one
     * for the caches.
     */
    static final long TIMEOUT_MILLIS = 18000;
    /**
     * Max. number to retry write or read operations
     */
    static final int MAX_RETRY_COUNT = 3;
    /**
     * List of level 2 caches, the client knows about
     */
    private List<ActorRef> l2Caches;
    /**
     * WriteMessage retry count
     */
    private int writeRetryCount = 0;
    /**
     * Is this Node waiting for a write-confirm message
     */
    private boolean isWaitingForWriteConfirm = false;
    /**
     * All unconfirmed read operations for the given key.
     * The value is the count of retries.
     */
    private final Map<Integer, Integer> unconfirmedReads = new HashMap<>();

    public Client(String id) {
        super(id);
    }

    static public Props props(String id) {
        return Props.create(Client.class, () -> new Client(id));
    }

    @Override
    protected long getTimeoutMillis() {
        return TIMEOUT_MILLIS;
    }

    /**
     * Returns a random actor from the given group.
     *
     * @param group A group of actors
     * @return A random instance from the given group
     */
    private ActorRef getRandomActor(List<ActorRef> group) {
        Random rand = new Random();
        List<ActorRef> groupClone = group;
        //Collections.shuffle(groupClone); // todo fix shuffle
        return groupClone.get(rand.nextInt(groupClone.size()));
    }

    /**
     * Sends a WriteMessage instance to the given L2 cache.
     * It also starts a write-timeout.
     *
     * @param l2Cache The choosen L2 cache actor
     * @param key     Key that has to be written
     * @param value   Value used to update the key
     */
    private void sendWriteMessage(ActorRef l2Cache, int key, int value, MessageConfig messageConfig) {
        WriteMessage writeMessage = new WriteMessage(key, value, messageConfig);
        Logger.write(this.id, LoggerOperationType.SEND, key, value, false, writeMessage.getUuid());
        this.send(writeMessage, l2Cache);
        // set timeout
        this.setTimeout(writeMessage, l2Cache, MessageType.WRITE);
        // set config
        this.isWaitingForWriteConfirm = true;
    }

    /**
     * Sends a CritWriteMessage instance to the given L2 cache.
     * It also starts a write-timeout.
     *
     * @param l2Cache The choosen L2 cache actor
     * @param key     Key that has to be written
     * @param value   Value used to update the key
     */
    private void sendCritWriteMessage(ActorRef l2Cache, int key, int value, MessageConfig messageConfig) {
        CritWriteMessage critWriteMessage = new CritWriteMessage(key, value, messageConfig);
        this.send(critWriteMessage, l2Cache);
        // set timeout
        this.setTimeout(critWriteMessage, l2Cache, MessageType.CRITICAL_WRITE);
        // set config
        this.isWaitingForWriteConfirm = true;
    }

    /**
     * Resends a WriteMessage to a random actor that is not the given unreachable actor.
     * Additionally, it increases the write-retry-count.
     *
     * @param unreachableActor The previously tried unreachable L2 cache
     * @param key              Key that has to be written
     * @param value            Value used to update the key
     * @param isCritical       Determines if the message is of critical nature
     */
    /*private void retryWriteMessage(ActorRef unreachableActor, int key, int value, boolean isCritical) {
        if (this.writeRetryCount < MAX_RETRY_COUNT) {
            // get random actor
            List<ActorRef> workingL2Caches = this.l2Caches
                    .stream().filter((actorRef -> actorRef != unreachableActor)).toList();
            ActorRef randomActor = this.getRandomActor(workingL2Caches);
            // increase count
            this.writeRetryCount = this.writeRetryCount + 1;
            // send message
            if (isCritical) {
                this.sendCritWriteMessage(randomActor, key, value, MessageConfig.none());
                Logger.criticalWrite(this.id, LoggerOperationType.RETRY, key, value, false);
            } else {
                this.sendWriteMessage(randomActor, key, value, MessageConfig.none());
                Logger.write(this.id, LoggerOperationType.RETRY, key, value, false);
            }
        } else {
            // abort retry
            this.resetWriteConfig();
        }
    }*/

    /**
     * Resets all important configs to enable another write operation.
     */
    private void resetWriteConfig() {
        this.isWaitingForWriteConfirm = false;
        this.writeRetryCount = 0;
    }

    /**
     * Sends a ReadMessage to the given L2 cache. Additionally, it increases the read count
     * and start a timeout for the read message.
     *
     * @param l2Cache Target L2 cache
     * @param key     Key to be read
     */
    private void sendReadMessage(ActorRef l2Cache, int key, MessageConfig messageConfig) {
        ReadMessage readMessage = new ReadMessage(key, this.getUpdateCountOrElse(key), messageConfig);
        this.send(readMessage, l2Cache);
        // set config
        this.addUnconfirmedRead(key, l2Cache);
        // set timeout
        this.setTimeout(readMessage, l2Cache, MessageType.READ);
    }

    /**
     * Sends a CritReadMessage to the given L2 cache. Additionally, it increases the read count
     * and start a timeout for the read message.
     *
     * @param l2Cache Target L2 cache
     * @param key     Key to be read
     */
    private void sendCritReadMessage(ActorRef l2Cache, int key, MessageConfig messageConfig) {
        CritReadMessage critReadMessage = new CritReadMessage(key, this.getUpdateCountOrElse(key), messageConfig);
        this.send(critReadMessage, l2Cache);
        // set config
        this.addUnconfirmedRead(key, l2Cache);
        // set timeout
        this.setTimeout(critReadMessage, l2Cache, MessageType.CRITICAL_READ);
    }

    /**
     * Resends a ReadMessage to a random L2 cache that is not the given unreachable actor.
     * Additionally, it increases the retry count for the given key.
     *
     * @param unreachableActor The L2 cache that is unreachable
     * @param key              Key to be read
     * @param isCritical       Determines if the message is of critical nature
     */
    /*private void retryReadMessage(ActorRef unreachableActor, int key, boolean isCritical) {
        int retryCountForKey = this.getRetryCountForRead(key);
        if (retryCountForKey < MAX_RETRY_COUNT) {
            // get another actor (hoping it will work)
            List<ActorRef> workingL2Caches = this.l2Caches
                    .stream().filter((actorRef -> actorRef != unreachableActor)).toList();
            ActorRef randomActor = this.getRandomActor(workingL2Caches);


            boolean isLocked = this.isKeyLocked(key);
            boolean isUnconfirmed = this.isReadUnconfirmed(key);
            // send message
            if (isUnconfirmed) {
                if (isCritical) {
                    this.sendCritReadMessage(randomActor, key, MessageConfig.none());
                    Logger.criticalRead(this.id, LoggerOperationType.RETRY, key,
                            this.getUpdateCountOrElse(key),
                            this.getUpdateCountOrElse(key),
                            isLocked);
                } else {
                    this.sendReadMessage(randomActor, key, MessageConfig.none());
                    Logger.read(this.id, LoggerOperationType.RETRY, key,
                            this.getUpdateCountOrElse(key),
                            this.getUpdateCountOrElse(key),
                            isLocked,
                            true,
                            true);
                }
                this.increaseCountForUnconfirmedReadMessage(key);
            }
        } else {
            // abort retries
            this.removeUnconfirmedRead(key);
        }
    }*/

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
     * Event listener that is triggered when this actor receives a
     * JoinL2CachesMessage. Then it is supposed to join a group of L2
     * cache actor instances.
     *
     * @param message The received JoinL2CachesMessage
     */
    private void onJoinL2Caches(JoinL2CachesMessage message) {
        this.l2Caches = List.copyOf(message.getL2Caches());
        Logger.join(this.id, "L2 Caches", this.l2Caches.size());
    }

    /**
     * Listener that is triggered whenever this actor receives a InstantiateWriteMessage.
     * Then, the actor is supposed to send a WriteMessage to the given L2 cache.
     *
     * @param message The received InstantiateWriteMessage
     */
    private void onInstantiateWriteMessage(InstantiateWriteMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        boolean isCritical = message.isCritical();

        if (this.isWaitingForWriteConfirm) {
            Logger.error(this.id, LoggerOperationType.ERROR, MessageType.INIT_WRITE, key, false,
                    "Waiting for another write-confirm");
            return;
        }

        ActorRef l2Cache = message.getL2Cache();
        if (!this.l2Caches.contains(l2Cache)) {
            Logger.error(this.id, LoggerOperationType.ERROR, MessageType.INIT_WRITE, key, false, "L2 cache is unknown");
            return;
        }

        Logger.initWrite(this.id, key, value, isCritical);

        if (isCritical) {
            Logger.criticalWrite(this.id, message.getUuid(), LoggerOperationType.SEND, key, value, false);
            this.sendCritWriteMessage(l2Cache, key, value, message.getMessageConfig());
        } else {
            this.sendWriteMessage(l2Cache, key, value, message.getMessageConfig());
        }
    }

    /**
     * Listener that is triggered whenever this actor receives a WriteConfirmMessage.
     * Then, a previous WriteMessage has been sent successfully, and this actor needs
     * to update its value and stop the write-timeout.
     *
     * @param message The received WriteConfirmMessage
     */
    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();

        Logger.writeConfirm(this.id, message.getUuid(), LoggerOperationType.RECEIVED, key, value, this.getValueOrElse(key), updateCount,
                this.getUpdateCountOrElse(key));

        this.unlockKey(key);
        try {
            // update value
            this.setValue(key, value, updateCount);
            // reset config
            this.resetWriteConfig();
        } catch (IllegalAccessException e) {
            // nothing todo, timeout will handle it
        }
    }

    /**
     * Listener that is triggered whenever this actor receives an InstantiateReadMessage. Then,
     * this actor is supposed to send a ReadMessage to the given L2 cache actor, start the timeout and
     * increase the read counter.
     *
     * @param message The received InstantiateReadMessage.
     */
    private void onInstantiateReadMessage(InstantiateReadMessage message) {
        int key = message.getKey();
        boolean isCritical = message.isCritical();

        if (this.isWaitingForWriteConfirm) {
            Logger.error(this.id, LoggerOperationType.ERROR, MessageType.INIT_READ, key, false,
                    "Waiting for write-confirm");
            return;
        }

        ActorRef l2Cache = message.getL2Cache();
        if (!this.l2Caches.contains(l2Cache)) {
            Logger.error(this.id, LoggerOperationType.ERROR, MessageType.INIT_READ, key, false, "L2 is unknown");
            return;
        }

        Logger.initRead(this.id, key, isCritical);

        if (isCritical) {
            Logger.criticalRead(this.id, LoggerOperationType.SEND, key, this.getUpdateCountOrElse(key), this.getUpdateCountOrElse(key), false);
            this.sendCritReadMessage(l2Cache, key, message.getMessageConfig());
        } else {
            Logger.read(this.id, LoggerOperationType.SEND, key, this.getUpdateCountOrElse(key), this.getUpdateCountOrElse(key), this.isKeyLocked(key), true, false);
            this.sendReadMessage(l2Cache, key, message.getMessageConfig());
        }
    }

    /**
     * Event listener that is triggered whenever this actor receives a ReadReplyMessage
     * message. Then, a previous ReadMessage was sent successfully and this actor has to update
     * the value and stop the timer.
     *
     * @param message The received ReadMessage
     */
    private void onReadReplyMessage(ReadReplyMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();
        Logger.readReply(this.id, LoggerOperationType.RECEIVED, key, value, this.getValueOrElse(key), updateCount,
                this.getUpdateCountOrElse(key));

        try {
            // update value
            this.setValue(key, value, updateCount);
            // reset config
            this.removeUnconfirmedRead(key);
        } catch (IllegalAccessException e) {
            // nothing todo, timeout will handle it
        }
    }

    @Override
    protected void handleTimeoutMessage(TimeoutMessage message) {
        MessageType type = message.getType();
        if (type == MessageType.WRITE && this.isWaitingForWriteConfirm) {
            Logger.timeout(this.id, type);
            this.resetWriteConfig();
        } else if (type == MessageType.READ) {
            ReadMessage readMessage = (ReadMessage) message.getMessage();
            int key = readMessage.getKey();

            // if the key is in this map, then no ReadReply has been received for the key
            if (this.isReadUnconfirmed(key)) {
                Logger.timeout(this.id, type);
                this.removeUnconfirmedRead(key);
            }
        } else if (type == MessageType.CRITICAL_READ) {
            CritReadMessage critReadMessage = (CritReadMessage) message.getMessage();
            int key = critReadMessage.getKey();

            if (this.isReadUnconfirmed(key)) {
                Logger.timeout(this.id, type);
                this.removeUnconfirmedRead(key);
            }
        } else if (type == MessageType.CRITICAL_WRITE && this.isWaitingForWriteConfirm) {
            CritWriteMessage critWriteMessage = (CritWriteMessage) message.getMessage();
            Logger.timeout(this.id, type);
            this.resetWriteConfig();
        }
    }

    @Override
    protected void handleErrorMessage(ErrorMessage message) {
        MessageType messageType = message.getMessageType();
        int key = message.getKey();

        if (messageType == MessageType.READ || messageType == MessageType.CRITICAL_READ) {
            this.removeUnconfirmedRead(key);
        } else if (messageType == MessageType.WRITE || messageType == MessageType.CRITICAL_WRITE) {
            this.resetWriteConfig();
        }
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinL2CachesMessage.class, this::onJoinL2Caches)
                .match(InstantiateWriteMessage.class, this::onInstantiateWriteMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .match(InstantiateReadMessage.class, this::onInstantiateReadMessage)
                .match(ReadReplyMessage.class, this::onReadReplyMessage)
                .match(TimeoutMessage.class, this::onTimeoutMessage)
                .match(ErrorMessage.class, this::onErrorMessage)
                .build();
    }

}
