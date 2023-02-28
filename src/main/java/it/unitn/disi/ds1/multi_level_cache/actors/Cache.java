package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.io.Serializable;
import java.util.*;

public abstract class Cache extends AbstractActor {

    private Map<UUID, WriteMessage> writeQueue = new HashMap<>();
    private Map<UUID, ActorRef> writeConfirmQueue = new HashMap<>();
    private Map<Integer, ActorRef> readReplyQueue = new HashMap<>();
    private Map<Integer, ActorRef> fillQueue = new HashMap<>();
    private Map<Integer, RefillMessage> reFillQueue = new HashMap<>();
    private Map<Integer, Integer> cache = new HashMap<>();
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

    private int getValueForKey(int key) {
        return this.cache.get(key);
    }

    private void setValueForKey(int key, int value) {
        this.cache.put(key, value);
    }

    private void forwardMessageToNext(Serializable message) {
        this.next.tell(message, this.getSelf());
    }

    private void multicastToLastLevel(Serializable message) {
        for (ActorRef actor: this.previousLevelCaches) {
            actor.tell(message, this.getSelf());
        }
    }

    private void sendToNext(Serializable message) {
        this.next.tell(message, this.getSelf());
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

    /**
     * Responds the given message to all actors in the read queue
     * for the given key. Afterwards, it removes the actor from
     * the queue.
     *
     * @param key Key of the value that was requested
     * @param message Response message
     */
    private void responseToQueue(Map<Integer, List<ActorRef>> queue, int key, Serializable message) {
        if (queue.containsKey(key)) {
            List<ActorRef> actors = queue.get(key);

            // reply
            Iterator<ActorRef> iter = actors.iterator();
            while (iter.hasNext()) {
                ActorRef actor = iter.next();
                actor.tell(message, this.getSelf());
                iter.remove();
            }

            // reset queue if its empty
            if (actors.isEmpty()) {
                queue.remove(key);
            }
        } else {
            // todo throw error
        }
    }

    private void responseToReFillQueue(int key) {
        if (this.reFillQueue.containsKey(key)) {
            RefillMessage message = this.reFillQueue.get(key);
            for (ActorRef actor: this.previousLevelCaches) {
                actor.tell(message, this.getSelf());
            }
            this.reFillQueue.remove(key);
        } else {
            // todo send error
        }
    }

    private void responseToWriteQueue(UUID uuid) {
        if (this.writeQueue.containsKey(uuid)) {
            Iterator<UUID> iter = this.writeQueue.keySet().iterator();
            while (iter.hasNext()) {
                UUID current = iter.next();
                if (uuid.equals(current)) {
                    // send message to next
                    WriteMessage message = this.writeQueue.get(current);
                    this.next.tell(message, this.getSelf());
                    iter.remove();
                }
            }
        } else {
            // todo error
        }
    }

    private void addToQueue(Map<Integer, List<ActorRef>> queue, int key, ActorRef actor) {
        if (queue.containsKey(key)) {
            // append to existing list
            queue.get(key).add(actor);
        } else {
            // need to add new list
            List<ActorRef> actors = new ArrayList<>();
            actors.add(actor);
            queue.put(key, actors);
        }
    }

    private void addToWriteConfirmQueue(UUID uuid, ActorRef actor) {
        if (!this.writeConfirmQueue.containsKey(uuid)) {
            this.writeConfirmQueue.put(uuid, actor);
        } else {
            // todo Error this shouldn't be
        }
    }

    private void responseReadReplyMessage(int key) {
        if (this.readReplyQueue.containsKey(key)) {
            int value = this.getValueForKey(key);
            ActorRef client = this.readReplyQueue.get(key);
            ReadReplyMessage readReplyMessage = new ReadReplyMessage(key, value);
            client.tell(readReplyMessage, this.getSelf());
            this.readReplyQueue.remove(key);
        }
    }

    private void responseFillMessage(int key) {
        if (this.fillQueue.containsKey(key)) {
            int value = this.getValueForKey(key);
            ActorRef cache = this.fillQueue.get(key);
            FillMessage fillMessage = new FillMessage(key, value);
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

        /*
         This is a cache, so we have to forward the write-message until the database
         */
        // 1. Add to write queue
        this.writeQueue.put(uuid, message);
        // 2. Add sender to write confirm queue
        this.addToWriteConfirmQueue(uuid, this.getSender());
        // 3. forward message to next
        this.responseToWriteQueue(uuid);
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
            if (this.cache.containsKey(key)) {
                this.setValueForKey(key, message.getValue());
            }
            // 2. Response confirm to sender
            Optional<ActorRef> sender = this.responseWriteConfirmQueue(uuid, message);
            // 3. Send refill to all other lower level caches (if lower caches exist)
            // todo make responseToRefillQueue
            if (!this.isLastLevelCache && sender.isPresent()) {
                for (ActorRef lastCache: this.previousLevelCaches) {
                    if (lastCache != sender.get()) {
                        RefillMessage reFillMessage = new RefillMessage(key, message.getValue());
                        lastCache.tell(reFillMessage, this.getSelf());
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
        if (this.cache.containsKey(key)) {
            // we need to update
            this.setValueForKey(key, message.getValue());
        } else {
            // never known this key, don't update
            System.out.printf("%s never read/write key %d, therefore no update\n", this.id, key);
        }

        // 2. Now forward to previous level Caches
        if (!this.isLastLevelCache) {
            // 1. Add refill message to queue
            this.reFillQueue.put(key, message);
            // 2. Dequeue refill message queue
            this.responseToReFillQueue(key);
            System.out.printf("%s need to reply to last level, count: %d\n", this.id, this.previousLevelCaches.size());
        }
    }

    private void onReadMessage(ReadMessage message) {
        int key = message.getKey();

        // add sender to queue
        if (!this.isLastLevelCache) {
            // add l2 cache to fill queue
            this.fillQueue.put(key, this.getSender());
        } else {
            // add to read reply queue, only for last level cache, so we can reply to client
            this.readReplyQueue.put(key, this.getSender());
        }

        // todo Check if our value is newer than the one by the client, if not, forward read message to get newest value
        // check if we own a more recent value
        if (this.cache.containsKey(key)) { // AND IS OUR NEWER OR EQUAL TO CLIENT ONE
            int wanted = this.getValueForKey(key);
            System.out.printf("%s already knows %d (%d)\n", this.id, key, wanted);
            // response accordingly
            this.responseForFillOrReadReply(key);
        } else {
            System.out.printf("%s does not know about %d, forward to next\n", this.id, key);
            // forward message to next
            this.forwardMessageToNext(message);
        }
    }

    private void onFillMessage(FillMessage message) {
        int key = message.getKey();
        System.out.printf("%s received fill message for {%d: %d}\n", this.id, message.getKey(), message.getValue());
        // Update value
        this.setValueForKey(key, message.getValue());
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
