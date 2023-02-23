package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinActorMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteConfirmMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteMessage;

import java.util.UUID;

public class L2Cache extends Cache {

    private ActorRef l1Cache;

    public L2Cache(int l1Id, int id) {
        super(String.format("L2-%d-%d", l1Id, id));
    }

    static public Props props(int l1Id, int id) {
        return Props.create(L2Cache.class, () -> new L2Cache(l1Id, id));
    }

    private void onJoinL1Cache(JoinActorMessage message) {
        this.l1Cache = message.getActor();
        System.out.printf("%s joined L1 cache\n", this.id);
    }

    @Override
    protected void forwardWriteToNext(WriteMessage message) {
        this.l1Cache.tell(message, this.getSelf());
    }

    @Override
    protected void forwardConfirmWriteToSender(WriteConfirmMessage message) {
        ActorRef client = this.writeHistory.get(message.getWriteMessageUUID());
        client.tell(message, this.getSelf());
    }

    @Override
    protected void addToWriteHistory(UUID uuid) {
        this.writeHistory.put(uuid, this.getSender());
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
