package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Database extends AbstractActor {

    private List<ActorRef> l1Caches;
    private Map<Integer, Integer> data = new HashMap<>();

    public Database() {
    }

    static public Props props() {
        return Props.create(Database.class, () -> new Database());
    }

    private void onJoinL1Caches(JoinGroupMessage message) {
        this.l1Caches = List.copyOf(message.getGroup());
        System.out.printf("Database joined group of %d L1 caches\n", this.l1Caches.size());
    }

    private void onWriteMessage(WriteMessage message) {
        System.out.printf("Database received write message of {%d: %d}\n", message.getKey(), message.getValue());

        // 1. save new data
        this.data.put(message.getKey(), message.getValue());

        // 2. send confirm to L1 sender
        WriteConfirmMessage writeConfirmMessage = new WriteConfirmMessage(message.getUuid(), message.getKey(), message.getValue());
        this.getSender().tell(writeConfirmMessage, this.getSelf());

        // 3. send refill to all other L1 caches
        RefillMessage refillMessage = new RefillMessage(message.getKey(), message.getValue());
        for (ActorRef l1Cache: this.l1Caches) {
            if (l1Cache != this.getSender()) {
                l1Cache.tell(refillMessage, this.getSelf());
            }
        }
    }

    private void onReadMessage(ReadMessage message) {
        int key = message.getKey();
        System.out.printf("Database received read message for key %d\n", key);
        if (this.data.containsKey(key)) {
            int value = this.data.get(key);
            System.out.printf("Requested value is %d, send fill message to L1 Cache\n", value);
            FillMessage fillMessage = new FillMessage(key, value);
            this.getSender().tell(fillMessage, this.getSelf());
        } else {
            System.out.printf("Database does not know about key %d\n", key);
            // todo send error response
        }
    }

    @Override
    public Receive createReceive() {
       return this
               .receiveBuilder()
               .match(JoinGroupMessage.class, this::onJoinL1Caches)
               .match(WriteMessage.class, this::onWriteMessage)
               .match(ReadMessage.class, this::onReadMessage)
               .build();
    }

}
