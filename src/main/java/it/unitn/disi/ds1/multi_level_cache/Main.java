package it.unitn.disi.ds1.multi_level_cache;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.environment.ActorEnvironment;

import java.util.NoSuchElementException;

public class Main {

    /**
     * number of level 1 caches
     */
    private static final Integer numberOfL1Caches = 2;
    /**
     * number of level 2 caches (for each level 1 cache)
     */
    private static final Integer numberOfL2Caches = 4;
    /**
     * number of clients
     */
    private static final Integer numberOfClients = 2;

    public static void main(String[] args) throws InterruptedException {
        ActorEnvironment actorEnvironment = new ActorEnvironment(
                "Multi-Level-Cache", numberOfL1Caches, numberOfL2Caches, numberOfClients);

        try {
            ActorRef firstClient = actorEnvironment.getClient(0).orElseThrow();
            ActorRef secondClient = actorEnvironment.getClient(0).orElseThrow();
            ActorRef l211 = actorEnvironment.getL2Cache(0).orElseThrow(); // L2-1-1
            ActorRef l221 = actorEnvironment.getL2Cache(4).orElseThrow(); // L2-2-1

            Thread.sleep(2000);
            System.out.println("### WRITE KEY 3 ###");
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            Thread.sleep(2000);
            System.out.println("### READ KEY 3 ###");
            actorEnvironment.makeClientRead(secondClient, l221, 3);
            Thread.sleep(2000);
            System.out.println("### READ KEY 3 AGAIN ###");
            actorEnvironment.makeClientRead(secondClient, l221, 3);
            Thread.sleep(2000);
            System.out.println("### MAKE L2-2-1 crash");
            actorEnvironment.makeCacheCrash(l221);
            Thread.sleep(2000);
            System.out.println("### READ KEY 3 AGAIN ###");
            actorEnvironment.makeClientRead(secondClient, l221, 3);
        } catch (NoSuchElementException exc) {
            // WHAT TODO??
        } catch (InterruptedException exc) {
            System.exit(1);
        }

        // Extra long sleep to make sure no message is still around
        Thread.sleep(10000);
        System.exit(0);
    }

}
