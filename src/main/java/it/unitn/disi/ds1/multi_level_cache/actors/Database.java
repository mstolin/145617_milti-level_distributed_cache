package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;

public class Database extends AbstractActor {

    private Map<Integer, Integer> data;

    public Database() {
        this.data = new HashMap<>();
    }

    static public Props props() {
        return Props.create(Database.class, () -> new Database());
    }

    @Override
    public Receive createReceive() {
        return null;
    }

}
