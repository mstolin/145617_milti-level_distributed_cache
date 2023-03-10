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

    private static void sleepAndDelimiter(int milliseconds) throws InterruptedException {
        System.out.println("----------------------");
        Thread.sleep(2000);
    }

    public static void main(String[] args) throws InterruptedException {
        ActorEnvironment actorEnvironment = new ActorEnvironment(
                "Multi-Level-Cache", numberOfL1Caches, numberOfL2Caches, numberOfClients);

        try {
            ActorRef firstClient = actorEnvironment.getClient(0).orElseThrow();
            ActorRef secondClient = actorEnvironment.getClient(0).orElseThrow();
            ActorRef l11 = actorEnvironment.getL1Cache(0).orElseThrow(); // L1-1
            ActorRef l12 = actorEnvironment.getL1Cache(1).orElseThrow(); // L1-2
            ActorRef l211 = actorEnvironment.getL2Cache(0).orElseThrow(); // L2-1-1
            ActorRef l212 = actorEnvironment.getL2Cache(1).orElseThrow(); // L2-1-2
            ActorRef l221 = actorEnvironment.getL2Cache(4).orElseThrow(); // L2-2-1
            ActorRef l222 = actorEnvironment.getL2Cache(5).orElseThrow(); // L2-2-2

            /*
            READ A VALUE
             */
            /*Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 9);*/

            /*
            READ A VALUE FROM SAME L2
             */
            /*
            actorEnvironment.makeClientRead(firstClient, l211, 3);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 3);*/

            /*
            READ A VALUE FROM TWICE FROM DIFFERENT L2 BUT SAME L1
             */
            /*Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 5);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l212, 5);*/

            /*
            READ A VALUE WITH CRASHED L1
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeCacheCrash(l12, 5);
            sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l221, 9);*/

            /*
            READ A VALUE WITH CRASHED L2
             */
            sleepAndDelimiter(2000);
            actorEnvironment.makeCacheCrash(l12, 10);
            sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l221, 1);

            /*
            WRITE A VALUE
             */
            /*Thread.sleep(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);*/

            /*
            WRITE A VALUE AND IMMEDIATELY INSTANTIATE ANOTHER WRITE MESSAGE
             */
            /*Thread.sleep(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            actorEnvironment.makeClientWrite(firstClient, l211, 4, 100);*/

            /*
            WRITE A VALUE AND IMMEDIATELY INSTANTIATE ANOTHER READ MESSAGE
             */
            /*Thread.sleep(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            actorEnvironment.makeClientRead(firstClient, l211, 4);*/

            /*
            WRITE A VALUE AND IMMEDIATELY INSTANTIATE ANOTHER WRITE MESSAGE, WAIT AND SEND ANOTHER WRITE
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            actorEnvironment.makeClientWrite(firstClient, l211, 4, 100);
            sleepAndDelimiter(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 4, 100);
            sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l221, 4);
            actorEnvironment.makeClientRead(firstClient, l212, 4);*/

            /*
            WRITE A VALUE, THEN READ FROM L2, THEN READ FROM DIFFERENT L2 WITH DIFFERENT L1
             */
            /*Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 3);
            Thread.sleep(2000);
            System.out.println("-------");
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            Thread.sleep(2000);
            System.out.println("-------");
            actorEnvironment.makeClientRead(firstClient, l211, 3);
            Thread.sleep(2000);
            System.out.println("-------");
            actorEnvironment.makeClientRead(firstClient, l221, 3);*/

            /*
            WRITE A VALUE WITH CRASHED L2
             */

            /*
            WRITE A VALUE WITH CRASHED L1
             */

            /*Thread.sleep(2000);
            System.out.println("### WRITE KEY 3 ###");
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            Thread.sleep(2000);
            System.out.println("### READ KEY 3 ###");
            actorEnvironment.makeClientRead(secondClient, l221, 3);
            Thread.sleep(2000);
            System.out.println("### READ KEY 3 AGAIN ###");
            actorEnvironment.makeClientRead(secondClient, l221, 3);
            Thread.sleep(2000);*/
            /*System.out.println("### MAKE L2-2-1 crash");
            actorEnvironment.makeCacheCrash(l221);
            System.out.println("### MAKE L1-1 crash");
            actorEnvironment.makeCacheCrash(l11);
            Thread.sleep(2000);
            System.out.println("### READ KEY 3 AGAIN ###");
            actorEnvironment.makeClientRead(secondClient, l211, 3);*/
        } catch (NoSuchElementException exc) {
            System.out.println("Some actor is not available");
        } catch (InterruptedException exc) {
            System.exit(1);
        }

        // Extra long sleep to make sure no message is still around
        Thread.sleep(15000);
        System.exit(0);
    }

}
