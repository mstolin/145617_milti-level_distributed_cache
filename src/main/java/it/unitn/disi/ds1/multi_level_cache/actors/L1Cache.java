package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.util.List;
import java.util.UUID;

public class L1Cache extends Cache {

    private List<ActorRef> l2Caches;
    private ActorRef database;

    public L1Cache(int id) {
        super(String.format("L1-%d", id));
    }

    static public Props props(int id) {
        return Props.create(L1Cache.class, () -> new L1Cache(id));
    }

    private void onJoinL2Cache(JoinGroupMessage message) {
        this.l2Caches = List.copyOf(message.getGroup());
        System.out.printf("%s joined group of %d L2 caches\n", this.id, this.l2Caches.size());
    }

    private void onJoinDatabase(JoinActorMessage message) {
        this.database = message.getActor();
        System.out.printf("%s joined group of database\n", this.id);
    }

    @Override
    protected void forwardWriteToNext(WriteMessage message) {
        this.database.tell(message, this.getSelf());
    }

    @Override
    protected void forwardConfirmWriteToSender(WriteConfirmMessage message) {
        ActorRef l2Cache = this.writeHistory.get(message.getWriteMessageUUID());
        l2Cache.tell(message, this.getSelf());
    }

    @Override
    protected void addToWriteHistory(UUID uuid) {
        this.writeHistory.put(uuid, this.getSender());
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinGroupMessage.class, this::onJoinL2Cache)
                .match(JoinActorMessage.class, this::onJoinDatabase)
                .match(WriteMessage.class, this::onWriteMessage)
                .match(WriteConfirmMessage.class, this::onWriteConfirmMessage)
                .match(RefillMessage.class, this::onRefillMessage)
                .build();
    }
}
