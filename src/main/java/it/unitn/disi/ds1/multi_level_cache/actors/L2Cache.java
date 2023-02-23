package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinActorMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteConfirmMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class L2Cache extends AbstractActor {

    final String id;
    private ActorRef l1Cache;
    private Map<UUID, ActorRef> writeHistory = new HashMap<>();

    public L2Cache(int l1Id, int id) {
        this.id = String.format("L2-%d-%d", l1Id, id);
    }

    static public Props props(int l1Id, int id) {
        return Props.create(L2Cache.class, () -> new L2Cache(l1Id, id));
    }

    private void onJoinL1Cache(JoinActorMessage message) {
        this.l1Cache = message.getActor();
        System.out.printf("%s joined L1 cache\n", this.id);
    }

    private void onWriteMessage(WriteMessage message) {
        // just forward message for now
        System.out.printf("%s received write message (%s), forward to L1 cache\n", this.id, message.getUuid().toString());
        this.writeHistory.put(message.getUuid(), this.getSender());
        this.l1Cache.tell(message, getSelf());
    }

    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        System.out.printf("%s received write confirm message (%s), forward to client\n", this.id, message.getWriteMessageUUID().toString());
        // todo Check if key exists
        ActorRef client = this.writeHistory.get(message.getWriteMessageUUID());
        this.writeHistory.remove(message.getWriteMessageUUID());
        client.tell(message, this.getSelf());
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinActorMessage.class, this::onJoinL1Cache)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .build();
    }

}
