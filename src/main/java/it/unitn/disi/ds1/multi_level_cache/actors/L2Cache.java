package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinActorMessage;

public class L2Cache extends AbstractActor {

    final Integer id;
    private ActorRef l1Cache;

    public L2Cache(int id) {
        this.id = id;
    }

    static public Props props(int id) {
        return Props.create(L2Cache.class, () -> new L2Cache(id));
    }

    private void onJoinL1Cache(JoinActorMessage message) {
        this.l1Cache = message.getActor();
        System.out.printf("L2 Cache %d joined L1 cache\n", this.id);
    }

    @Override
    public Receive createReceive() {
        return this
                .receiveBuilder()
                .match(JoinActorMessage.class, this::onJoinL1Cache)
                .build();
    }

}
