package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinGroupMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Database extends AbstractActor {

    private Map<Integer, Integer> data;
    private List<ActorRef> l1Caches;

    public Database() {
        this.data = new HashMap<>();
    }

    static public Props props() {
        return Props.create(Database.class, () -> new Database());
    }

    private void onJoinL1Caches(JoinGroupMessage message) {
        this.l1Caches = List.copyOf(message.getGroup());
        System.out.printf("Database joined group of %d L1 caches\n", this.l1Caches.size());
    }

    @Override
    public Receive createReceive() {
       return this
               .receiveBuilder()
               .match(JoinGroupMessage.class, this::onJoinL1Caches)
               .build();
    }

}
