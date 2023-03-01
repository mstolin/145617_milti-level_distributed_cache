package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.io.Serializable;
import java.util.*;

public abstract class Cache extends AbstractActor {

    private Map<UUID, ActorRef> writeConfirmQueue = new HashMap<>();
    private Map<Integer, ActorRef> readReplyQueue = new HashMap<>();
    private Map<Integer, ActorRef> fillQueue = new HashMap<>();
    private DataStore data = new DataStore();
    /**
     * Determines if this is the last level cache.
     * If true, this is the cache the clients talk to.
     */
    protected boolean isLastLevelCache = false;
    /**
     * A list of all actors of the previous level.
     * Empty if this is a last level cache.
     */
    private List<ActorRef> previousLevelCaches;
    /** This is either the next level cache or the database */
    private ActorRef next;

    public final String id;

    public Cache(String id) {
        this.id = id;
    }

    private void forwardMessageToNext(Serializable message) {
        this.next.tell(message, this.getSelf());
    }

    private void multicast(Serializable message, List<ActorRef> group) {
        for (ActorRef actor: group) {
            actor.tell(message, this.getSelf());
        }
    }

    private Optional<ActorRef> responseWriteConfirmQueue(UUID uuid, WriteConfirmMessage message) {
        if (this.writeConfirmQueue.containsKey(uuid)) {
            ActorRef actor = this.writeConfirmQueue.get(uuid);
            actor.tell(message, this.getSelf());
            this.writeConfirmQueue.remove(uuid);
            return Optional.of(actor);
        }
        return Optional.empty();
    }

    private void responseReadReplyMessage(int key) {
        Optional<Integer> value = this.data.getValueForKey(key);
        Optional<Integer> updateCount = this.data.getUpdateCountForKey(key);

        if (this.readReplyQueue.containsKey(key) && value.isPresent() && updateCount.isPresent()) {
            ActorRef client = this.readReplyQueue.get(key);
            ReadReplyMessage readReplyMessage = new ReadReplyMessage(key, value.get(), updateCount.get());
            client.tell(readReplyMessage, this.getSelf());
            this.readReplyQueue.remove(key);
        }
    }

    private void responseFillMessage(int key) {
        Optional<Integer> value = this.data.getValueForKey(key);
        Optional<Integer> updateCount = this.data.getUpdateCountForKey(key);

        if (this.fillQueue.containsKey(key) && value.isPresent() && updateCount.isPresent()) {
            ActorRef cache = this.fillQueue.get(key);
            FillMessage fillMessage = new FillMessage(key, value.get(), updateCount.get());
            cache.tell(fillMessage, this.getSelf());
            this.fillQueue.remove(key);
        }
    }

    private void responseForFillOrReadReply(int key) {
        if (!this.isLastLevelCache) {
            // forward fill to l2 cache
            this.responseFillMessage(key);
        } else {
            // answer read reply to client
            this.responseReadReplyMessage(key);
        }
    }

    protected void onJoinNext(JoinActorMessage message) {
        this.next = message.getActor();
        System.out.printf("%s joined group of next actor\n", this.id);
    }

    private void onJoinPreviousGroup(JoinGroupMessage message) {
        this.previousLevelCaches = List.copyOf(message.getGroup());
        System.out.printf("%s joined group of %d previous level caches\n",
                this.id, this.previousLevelCaches.size());
    }

    private void onWriteMessage(WriteMessage message) {
        UUID uuid = message.getUuid();
        System.out.printf("%s received write message (%s), forward to next\n", this.id, uuid.toString());

        // Regardless of level, we add the sender to the write-confirm-queue
        this.writeConfirmQueue.put(uuid, this.getSender());
        this.forwardMessageToNext(message);
    }

    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        UUID uuid = message.getWriteMessageUUID();
        int key = message.getKey();
        System.out.printf(
                "%s received write confirm message (%s), forward to sender\n",
                this.id, uuid.toString());

        if (this.writeConfirmQueue.containsKey(uuid)) {
            /*
            Message id is known. First, update cache, forward confirm to sender,
            lastly remove message from our history.
             */
            System.out.printf("%s need to forward confirm message\n", this.id);

            // 1. Update value if needed
            if (this.data.containsKey(key)) {
                System.out.printf("%s Update value: {%d :%d} (given UC: %d, my UC: %d)\n",
                        this.id, key, message.getValue(), message.getUpdateCount(), this.data.getUpdateCountForKey(key).get());
                this.data.setValueForKey(key, message.getValue(), message.getUpdateCount());
            }

            // 2. Response confirm to sender
            Optional<ActorRef> sender = this.responseWriteConfirmQueue(uuid, message);

            // 3. Send refill to all other lower level caches (if lower caches exist)
            if (!this.isLastLevelCache && sender.isPresent()) {
                RefillMessage reFillMessage = new RefillMessage(key, message.getValue(), message.getUpdateCount());
                for (ActorRef cache: this.previousLevelCaches) {
                    if (cache != sender.get()) {
                        cache.tell(reFillMessage, this.getSelf());
                    }
                }
            }
        } else {
            // todo Error this shouldn't be
        }
    }

    private void onRefillMessage(RefillMessage message) {
        int key = message.getKey();
        System.out.printf(
                "%s received refill message for key %d. Update if needed.\n",
                this.id, key);

        // 1. Update if needed
        if (this.data.containsKey(key)) {
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
            System.out.printf("%s need to reply to last level, count: %d\n", this.id, this.previousLevelCaches.size());
            this.multicast(message, this.previousLevelCaches);
        }
    }

    private void onReadMessage(ReadMessage message) {
        int key = message.getKey();
        int updateCount = message.getUpdateCount();

        // add sender to queue
        if (!this.isLastLevelCache) {
            // add l2 cache to fill queue
            this.fillQueue.put(key, this.getSender());
        } else {
            // add to read reply queue, only for last level cache, so we can reply to client
            this.readReplyQueue.put(key, this.getSender());
        }

        Optional<Integer> wanted = this.data.getValueForKey(key);
        // check if we own a more recent or an equal value
        if (wanted.isPresent() && this.data.isNewerOrEqual(key, updateCount)) {
            System.out.printf("%s knows an equal or more recent value: {%d :%d} (given UC: %d, my UC: %d)\n",
                    this.id, key, wanted.get(), updateCount, this.data.getUpdateCountForKey(key).get());
            // response accordingly
            this.responseForFillOrReadReply(key);
        } else {
            // We either know non or an old value, so forward message to next
            System.out.printf("%s does not know %d or has an older version (given UC: %d, my UC: %d), forward to next\n",
                    this.id, key, updateCount, this.data.getUpdateCountForKey(key).orElse(0));
            this.forwardMessageToNext(message);
        }
    }

    private void onFillMessage(FillMessage message) {
        /* No need to check the received update count. At this point, the received value is at least equal
        to our known value. See onReadMessage why.
         */
        int key = message.getKey();
        System.out.printf("%s received fill message for {%d: %d} (given UC: %d, my UC: %d)\n",
                this.id, message.getKey(), message.getValue(), message.getUpdateCount(), this.data.getUpdateCountForKey(key).orElse(0));
        // Update value
        this.data.setValueForKey(key, message.getValue(), message.getUpdateCount());
        // response accordingly
        this.responseForFillOrReadReply(key);
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinActorMessage.class, this::onJoinNext)
                .match(JoinGroupMessage.class, this::onJoinPreviousGroup)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .match(RefillMessage.class, this::onRefillMessage)
                .match(ReadMessage.class, this::onReadMessage)
                .match(FillMessage.class, this::onFillMessage)
                .build();
    }

}
