package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinActorMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteConfirmMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteMessage;

public class L2Cache extends AbstractActor {

    final String id;
    private ActorRef l1Cache;

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
        System.out.println("L2 hat bekommen\n");
        this.l1Cache.tell(message, getSelf());
    }

    private void onWriteConfirmMessage(WriteConfirmMessage message) {
        System.out.printf("%s received write confirm, forward to client\n", this.id);
        /*WriteMessage confirmed = message.getWriteMessage();
        System.out.printf("MESSAGE CONFIRMED %d: %d\n", confirmed.getKey(), confirmed.getValue());
        if (this.writeHistory.remove(confirmed)) {
            System.out.println("MESSAGE WAS REMOVED FROM HISTORY");
        }*/
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
