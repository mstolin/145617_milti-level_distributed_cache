package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.io.Serializable;
import java.util.*;

public abstract class Cache extends AbstractActor {

    private Map<UUID, ActorRef> writeConfirmQueue = new HashMap<>();
    private Map<Integer, List<ActorRef>> readReplyQueue = new HashMap<>();
    private Map<Integer, List<ActorRef>> fillQueue = new HashMap<>();
    private Map<Integer, List<ActorRef>> reFillQueue = new HashMap<>();
    protected Map<Integer, Integer> cache = new HashMap<>();
    /**
     * Determines if this is the last level cache.
     * If true, this is the cache the clients talk to.
     */
    protected boolean isLastLevelCache = false;
    /**
     * A list of all actors of the previous level.
     * Empty if this is a last level cache.
     */
    protected List<ActorRef> previousLevelCaches;
    /** This is either the next level cache or the database */
    protected ActorRef next;

    public final String id;

    public Cache(String id) {
        this.id = id;
    }

    protected int getValueForKey(int key) {
        return this.cache.get(key);
    }

    protected void setValueForKey(int key, int value) {
        this.cache.put(key, value);
    }

    protected void forwardMessageToNext(Serializable message) {
        this.next.tell(message, this.getSelf());
    }

    protected void replyToLastLevel(Serializable message) {
        for (ActorRef actor: this.previousLevelCaches) {
            actor.tell(message, this.getSelf());
        }
    }

    protected void responseWriteConfirmQueue(UUID uuid, WriteConfirmMessage message) {
        if (this.writeConfirmQueue.containsKey(uuid)) {
            ActorRef actor = this.writeConfirmQueue.get(uuid);
            actor.tell(message, this.getSelf());
            this.writeConfirmQueue.remove(uuid);
        }
    }

    protected void addToWriteConfirmQueue(UUID uuid, ActorRef actor) {
        if (!this.writeConfirmQueue.containsKey(uuid)) {
            this.writeConfirmQueue.put(uuid, actor);
        } else {
            // todo Error this shouldn't be
        }
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

    private void responseToReadQueue(int key, Serializable message) {
        this.responseToQueue(this.readReplyQueue, key, message);
    }

    private void responseToFillQueue(int key, Serializable message) {
        this.responseToQueue(this.fillQueue, key, message);
    }

    private void responseToReFillQueue(int key, Serializable message) {
        this.responseToQueue(this.reFillQueue, key, message);
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

    private void addToReadQueue(int key, ActorRef actor) {
        this.addToQueue(this.readReplyQueue, key, actor);
    }

    private void addToFillQueue(int key, ActorRef actor) {
        this.addToQueue(this.fillQueue, key, actor);
    }

    private void addToReFillQueue(int key, ActorRef actor) {
        this.addToQueue(this.reFillQueue, key, actor);
    }

    private void replyValueForKey(int key) {
        int wanted = this.getValueForKey(key);
        if (!this.isLastLevelCache) {
            // response to fill queue
            FillMessage fillMessage = new FillMessage(key, wanted);
            this.responseToFillQueue(key, fillMessage);
        } else {
            // response to read reply queue
            ReadReplyMessage replyMessage = new ReadReplyMessage(key, wanted);
            this.responseToReadQueue(key, replyMessage);
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

    protected void onWriteMessage(WriteMessage message) {
        UUID uuid = message.getUuid();

        // just forward message for now
        System.out.printf("%s received write message (%s), forward to next\n", this.id, uuid.toString());

        // 1. add to write confirm queue
        this.addToWriteConfirmQueue(uuid, this.getSender());
        // 2. forward message to next
        // todo make with queue
        this.forwardMessageToNext(message);
    }

    protected void onWriteConfirmMessage(WriteConfirmMessage message) {
        UUID uuid = message.getWriteMessageUUID();
        System.out.printf(
                "%s received write confirm message (%s), forward to sender\n",
                this.id, uuid.toString());

        if (this.writeConfirmQueue.containsKey(uuid)) {
            /*
            Message id is known. First, update cache, forward confirm to sender,
            lastly remove message from our history.
             */
            System.out.printf("%s need to forward confirm message\n", this.id);
            this.setValueForKey(message.getKey(), message.getValue());
            this.responseWriteConfirmQueue(uuid, message);

            // send refill to all other lower level caches
            // todo
        } else {
            // todo Error this shouldn't be
        }
    }

    protected void onRefillMessage(RefillMessage message) {

        // todo rename ReFill
        // todo Here check if we need to write confirm for the key

        int key = message.getKey();

        System.out.printf(
                "%s received refill message for key %d. Update if needed.\n",
                this.id, key);

        // 1. update if needed
        // todo make own function
        if (this.cache.containsKey(key)) {
            // we need to update
            this.setValueForKey(key, message.getValue());
        } else {
            // never known this key, don't update
            System.out.printf("%s never read/write key %d, therefore no update\n", this.id, key);
        }

        // 2. if not last level (L2), forward refill to previous level
        // todo think about a queue here
        if (!this.isLastLevelCache) {
            System.out.printf("%s need to reply to last level, count: %d\n", this.id, this.previousLevelCaches.size());
            this.replyToLastLevel(message);
        }
    }

    protected void onReadMessage(ReadMessage message) {
        int key = message.getKey();
        ActorRef sender = this.getSender();

        if (!this.isLastLevelCache) {
            // add l2 to fill queue
            this.addToFillQueue(key, sender);
        } else {
            // add client to read reply queue
            this.addToReadQueue(key, sender);
        }

        // check if we already have the value
        if (this.cache.containsKey(key)) {
            // directly response back
            int wanted = this.getValueForKey(key);
            System.out.printf("%s already knows %d (%d)\n", this.id, key, wanted);
            this.replyValueForKey(key);
        } else {
            // ask next level about it
            System.out.printf("%s does not know about %d, forward to next\n", this.id, key);
            this.forwardMessageToNext(message);
        }
    }

    protected void onFillMessage(FillMessage message) {
        int key = message.getKey();
        System.out.printf("%s received fill message for {%d: %d}\n", this.id, message.getKey(), message.getValue());
        this.cache.put(key, message.getValue());
        this.replyValueForKey(key);
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
