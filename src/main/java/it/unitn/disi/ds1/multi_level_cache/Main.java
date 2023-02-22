package it.unitn.disi.ds1.multi_level_cache;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.multi_level_cache.actors.Client;
import it.unitn.disi.ds1.multi_level_cache.actors.Database;
import it.unitn.disi.ds1.multi_level_cache.actors.L2Cache;
import it.unitn.disi.ds1.multi_level_cache.actors.L1Cache;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinActorMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinGroupMessage;

import java.util.ArrayList;
import java.util.List;

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
        ActorRef database = system.actorOf(Database.props());

        // Create the caches
        List<ActorRef> l1Caches = new ArrayList<>();
        for (int i = 0; i < numberOfL1Caches; i++) {
            // Create L1 Cache
            ActorRef l1Cache = system.actorOf(L1Cache.props(i));
            l1Caches.add(l1Cache);

            // Create L2 caches
            List<ActorRef> l2Caches = new ArrayList<>();
            for (int j = 0; j < numberOfL2Caches; j++) {
                // Create L2 Cache
                ActorRef l2Cache = system.actorOf(L2Cache.props(i));
                l2Caches.add(l2Cache);
                // Send join message to L2 cache to join the L1 cache
                JoinActorMessage joinL1CacheMessage = new JoinActorMessage(l1Cache);
                l2Cache.tell(joinL1CacheMessage, null);
            }
            l2Caches = List.copyOf(l2Caches);

            // Send join message to L1 cache to join l2 caches
            JoinGroupMessage joinL2CachesMessageMessage = new JoinGroupMessage(l2Caches);
            l1Cache.tell(joinL2CachesMessageMessage, null);
            // send join message to L1 Cache to join database
            JoinActorMessage joinDatabaseMessage = new JoinActorMessage(database);
            l1Cache.tell(joinDatabaseMessage, null);
        }
        l1Caches = List.copyOf(l1Caches);

        // Send join message to database to join all L1 caches
        JoinGroupMessage joinL1CachesMessageMessage = new JoinGroupMessage(l1Caches);
        database.tell(joinL1CachesMessageMessage, null);

        // Create the clients
        for (int i = 0; i < numberOfClients; i++) {
            system.actorOf(Client.props());
            // join l2 caches
        }
    }

    public static void main(String[] args) {
        // ## Actor system
        initActorSystem();
    }

}
