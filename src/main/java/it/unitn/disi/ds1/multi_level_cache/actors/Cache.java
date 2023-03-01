package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.io.Serializable;
import java.util.*;

public class Cache extends AbstractActor {

    private ActorRef database;
    private ActorRef mainL1Cache;
    /** Contains all L1 caches except the one defined in mainL1Cache */
    private List<ActorRef> l1Caches;
    private List<ActorRef> l2Caches;
    /**
     * Determines if this is the last level cache.
     * If true, this is the cache the clients talk to.
     */
    private final boolean isLastLevelCache;
    private Map<UUID, ActorRef> writeConfirmQueue = new HashMap<>();
    private Map<Integer, ActorRef> readReplyQueue = new HashMap<>();
    private Map<Integer, ActorRef> fillQueue = new HashMap<>();
    private DataStore data = new DataStore();
    /** This is either the next level cache or the database */
    private boolean hasCrashed = false;

    public final String id;

    static public Props props(String id, boolean isLastLevelCache) {
        return Props.create(Cache.class, () -> new Cache(id, isLastLevelCache));
    }

    public Cache(String id, boolean isLastLevelCache) {
        this.id = id;
        this.isLastLevelCache = isLastLevelCache;
    }

    private void forwardMessageToNext(Serializable message) {
        if (this.isLastLevelCache) {
            this.mainL1Cache.tell(message, this.getSelf());
        } else {
            this.database.tell(message, this.getSelf());
        }
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

    protected void onJoinDatabase(JoinDatabaseMessage message) {
        this.database = message.getDatabase();
        System.out.printf("%s joined database\n", this.id);
    }

    protected void onJoinMainL1Cache(JoinMainL1CacheMessage message) {
        this.mainL1Cache = message.getL1Cache();
        System.out.printf("%s joined main L1 cache\n", this.id);
    }

    private void onJoinL1Caches(JoinL1CachesMessage message) {
        this.l1Caches = message.getL1Caches();
        System.out.printf("%s joined group of %d L1 caches\n", this.id, this.l1Caches.size());
    }

    private void onJoinL2Caches(JoinL2CachesMessage message) {
        this.l2Caches = message.getL2Caches();
        System.out.printf("%s joined group of %d L2 caches\n", this.id, this.l2Caches.size());
    }

    private void onWriteMessage(WriteMessage message) {
        if (this.hasCrashed) {
            // Can't do anything
            return;
        }

        UUID uuid = message.getUuid();
        System.out.printf("%s received write message (%s), forward to next\n", this.id, uuid.toString());

        // Regardless of level, we add the sender to the write-confirm-queue
        this.writeConfirmQueue.put(uuid, this.getSender());
        this.forwardMessageToNext(message);
    }

    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        if (this.hasCrashed) {
            // Can't do anything
            return;
        }

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

            // 3. Send refill to all L2 caches (if lower caches exist)
            if (!this.isLastLevelCache && sender.isPresent()) {
                RefillMessage reFillMessage = new RefillMessage(key, message.getValue(), message.getUpdateCount());
                for (ActorRef cache: this.l2Caches) {
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
        if (this.hasCrashed) {
            // Can't do anything
            return;
        }

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
            System.out.printf("%s need to reply to L2 caches, count: %d\n", this.id, this.l2Caches.size());
            this.multicast(message, this.l2Caches);
        }
    }

    private void onReadMessage(ReadMessage message) {
        if (this.hasCrashed) {
            // Can't do anything
            return;
        }

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
        if (this.hasCrashed) {
            // Can't do anything
            return;
        }

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

    private void onCrashMessage(CrashMessage message) {
        this.hasCrashed = true;
        this.data.resetData();
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinDatabaseMessage.class, this::onJoinDatabase)
                .match(JoinMainL1CacheMessage.class, this::onJoinMainL1Cache)
                .match(JoinL1CachesMessage.class, this::onJoinL1Caches)
                .match(JoinL2CachesMessage.class, this::onJoinL2Caches)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .match(RefillMessage.class, this::onRefillMessage)
                .match(ReadMessage.class, this::onReadMessage)
                .match(FillMessage.class, this::onFillMessage)
                .match(CrashMessage.class, this::onCrashMessage)
                .build();
    }

}
