package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;

import java.io.Serializable;

public class JoinDatabaseMessage implements Serializable {

    private final ActorRef database;

    public JoinDatabaseMessage(ActorRef database) {
        this.database = database;
    }

    public ActorRef getDatabase() {
        return database;
    }

}
