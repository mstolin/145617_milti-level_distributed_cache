package it.unitn.disi.ds1.multi_level_cache;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.multi_level_cache.actors.L2Cache;
import it.unitn.disi.ds1.multi_level_cache.actors.L1Cache;

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
        //system.actorOf(Database.props());

        // Create the caches
        List<ActorRef> l1Caches = new ArrayList<>();
        for (int i = 0; i < numberOfL1Caches; i++) {
            List<ActorRef> l2Caches = new ArrayList<>();
            for (int j = 0; j < numberOfL2Caches; j++) {
                // Create L2 Cache
                ActorRef l2Cache = system.actorOf(L2Cache.props(i));
                l2Caches.add(l2Cache);
            }

            // Create L1 Cache
            ActorRef l1Cache = system.actorOf(L1Cache.props(i));
            l1Caches.add(l1Cache);
            // Send join message to L1 cache
            l2Caches = List.copyOf(l2Caches);
            L1Cache.JoinL2CacheMessage joinMessage = new L1Cache.JoinL2CacheMessage(l2Caches);
            l1Cache.tell(joinMessage, null);
        }

        // Create the clients
        /*for (int i = 0; i < numberOfClients; i++) {
            system.actorOf(Client.props());
        }*/
    }

    public static void main(String[] args) {
        // ## Actor system
        initActorSystem();
    }

}
