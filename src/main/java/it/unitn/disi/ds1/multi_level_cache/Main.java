package it.unitn.disi.ds1.multi_level_cache;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.multi_level_cache.actors.Cache;
import it.unitn.disi.ds1.multi_level_cache.actors.Client;
import it.unitn.disi.ds1.multi_level_cache.actors.Database;

public class Main {

    /** number of level 1 caches */
    private static final Integer numberOfL1Caches = 2;
    /** number of level 2 caches (for each level 1 cache) */
    private static final Integer numberOfL2Caches = 4;
    /** number of clients */
    private static final Integer numberOfClients = 4;

    private static void initActorSystem() {
        final ActorSystem system = ActorSystem.create("multi-level-cache");

        // Create a single database actor
        system.actorOf(Database.props());

        // Create the caches
        for (int i = 0; i < numberOfL1Caches; i++) {
            // Create L1 Cache
            ActorRef l1Cache = system.actorOf(Cache.props(i));

            for (int j = 0; j < numberOfL2Caches; j++) {
                // Create L2 Cache
                ActorRef l2Cache = system.actorOf(Cache.props(i));
            }
        }

        // Create the clients
        for (int i = 0; i < numberOfClients; i++) {
            system.actorOf(Client.props());
        }
    }

    public static void main(String[] args) {
        // ## Actor system
        initActorSystem();
    }

}
