package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class Database extends Node {

    private List<ActorRef> l1Caches;
    private List<ActorRef> l2Caches;
    private DataStore data = new DataStore();
    private Optional<ActorRef> writeConfirmActor = Optional.empty();
    private List<ActorRef> refillConfirmActors = new ArrayList<>();
    private List<ActorRef> fillConfirmActors = new ArrayList<>();

    public Database() {
        super("Database");
        this.setDefaultData(10);
    }

    static public Props props() {
        return Props.create(Database.class, () -> new Database());
    }

    private void setDefaultData(int size) {
        for (int i = 0; i < size; i++) {
            int value = new Random().nextInt(1000);
            this.data.setValueForKey(i, value, 0);
        }
    }

    private void onJoinL1Caches(JoinL1CachesMessage message) {
        this.l1Caches = List.copyOf(message.getL1Caches());
        System.out.printf("Database joined group of %d L1 caches\n", this.l1Caches.size());
    }

    private void onJoinL2Caches(JoinL2CachesMessage message) {
        this.l2Caches = List.copyOf(message.getL2Caches());
        System.out.printf("Database joined group of %d L2 caches\n", this.l2Caches.size());
    }

    private void onWriteMessage(WriteMessage message) {
        int key = message.getKey();
        int value = message.getValue();;
        System.out.printf("Database received write message of {%d: %d}\n", key, value);

        // 1. save new data
        this.data.setValueForKey(key, value);
        // we can be sure, since we set value previously
        int updateCount = this.data.getUpdateCountForKey(key).get();

        // 2. send confirm to L1 sender
        // todo make own method
        WriteConfirmMessage writeConfirmMessage = new WriteConfirmMessage(
                message.getUuid(), message.getKey(), message.getValue(), updateCount);
        this.getSender().tell(writeConfirmMessage, this.getSelf());
        this.writeConfirmActor = Optional.of(this.getSender());
        this.setTimeout(writeConfirmMessage, this.getSender(), TimeoutType.WRITE_CONFIRM);

        // 3. send refill to all other L1 caches
        // todo make own method
        RefillMessage refillMessage = new RefillMessage(message.getKey(), message.getValue(), updateCount);
        for (ActorRef l1Cache: this.l1Caches) {
            if (l1Cache != this.getSender()) {
                l1Cache.tell(refillMessage, this.getSelf());
                this.setTimeout(refillMessage, l1Cache, TimeoutType.RE_FILL);
            }
        }
    }

    private void onReadMessage(ReadMessage message) {
        int key = message.getKey();
        System.out.printf("Database received read message for key %d\n", key);

        Optional<Integer> value = this.data.getValueForKey(key);
        Optional<Integer> updateCount = this.data.getUpdateCountForKey(key);
        if (value.isPresent() && updateCount.isPresent()) {
            System.out.printf("Requested value is %d, send fill message to sender\n", value.get());
            FillMessage fillMessage = new FillMessage(key, value.get(), updateCount.get());
            this.getSender().tell(fillMessage, this.getSelf());
            this.setTimeout(fillMessage, this.getSender(), TimeoutType.FILL);
        } else {
            System.out.printf("Database does not know about key %d\n", key);
            // todo send error response
        }
    }

    @Override
    protected void onTimeoutMessage(TimeoutMessage message) {
        System.out.println("Database - timed-out");


        // a L1 cache has timed-out, therefore send flush message to all L2s
        // todo make own method flushL2Caches()
        FlushMessage flushMessage = new FlushMessage(message.getUnreachableActor());
        for (ActorRef l2Cache: this.l2Caches) {
            l2Cache.tell(flushMessage, this.getSelf());

            /*
            Wenn ein L1 crashs dann flush an alle L2, die check war das mein L1 dann flush.
            Client wird so oder so timeouten und nachricht neu senden
             */
        }
    }

    @Override
    public Receive createReceive() {
       return this
               .receiveBuilder()
               .match(JoinL1CachesMessage.class, this::onJoinL1Caches)
               .match(JoinL2CachesMessage.class, this::onJoinL2Caches)
               .match(WriteMessage.class, this::onWriteMessage)
               .match(ReadMessage.class, this::onReadMessage)
               .build();
    }

}
