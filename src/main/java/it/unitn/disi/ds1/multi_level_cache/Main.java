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
        Thread.sleep(milliseconds);
    }

    public static void main(String[] args) throws InterruptedException {
        ActorEnvironment actorEnvironment = new ActorEnvironment(
                "Multi-Level-Cache", numberOfL1Caches, numberOfL2Caches, numberOfClients);

        try {
            ActorRef firstClient = actorEnvironment.getClient(0).orElseThrow();
            ActorRef secondClient = actorEnvironment.getClient(1).orElseThrow();
            ActorRef l11 = actorEnvironment.getL1Cache(0).orElseThrow(); // L1-1
            ActorRef l12 = actorEnvironment.getL1Cache(1).orElseThrow(); // L1-2
            ActorRef l211 = actorEnvironment.getL2Cache(0).orElseThrow(); // L2-1-1
            ActorRef l212 = actorEnvironment.getL2Cache(1).orElseThrow(); // L2-1-2
            ActorRef l213 = actorEnvironment.getL2Cache(2).orElseThrow(); // L2-1-3
            ActorRef l214 = actorEnvironment.getL2Cache(3).orElseThrow(); // L2-1-4
            ActorRef l221 = actorEnvironment.getL2Cache(4).orElseThrow(); // L2-2-1
            ActorRef l222 = actorEnvironment.getL2Cache(5).orElseThrow(); // L2-2-2
            ActorRef l223 = actorEnvironment.getL2Cache(6).orElseThrow(); // L2-2-3
            ActorRef l224 = actorEnvironment.getL2Cache(7).orElseThrow(); // L2-2-4

            // sleep for 2 seconds to guarantee the infrastructure has been built successfully
            Thread.sleep(2000);

            /*
            READ A VALUE
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 9);*

            /*
            READ A VALUE FROM SAME L2
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 3);
            sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 3);*/

            /*
            READ A VALUE FROM TWICE FROM DIFFERENT L2 BUT SAME L1
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 5);
            sleepAndDelimiter(2000);
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
            /*Thread.sleep(2000);
            actorEnvironment.makeCacheCrash(l221, 10);
            actorEnvironment.makeClientRead(firstClient, l221, 1);*/

            /*
            WRITE A VALUE
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);*/

            /*
            WRITE A VALUE AND IMMEDIATELY INSTANTIATE ANOTHER WRITE MESSAGE
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            actorEnvironment.makeClientWrite(firstClient, l211, 4, 100);*/

            /*
            WRITE A VALUE AND IMMEDIATELY INSTANTIATE ANOTHER READ MESSAGE
             */
            /*sleepAndDelimiter(2000);
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
            // todo fix this
            /*actorEnvironment.makeClientRead(firstClient, l211, 3);
            Thread.sleep(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 3);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l221, 3);*/

            /*
            WRITE A VALUE WITH CRASHED L2
             */
            /*Thread.sleep(2000);
            actorEnvironment.makeCacheCrash(l211, 10);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);*/

            /*
            WRITE A VALUE WITH ONLY CRASHED L2
             */
            /*actorEnvironment.makeCacheCrash(l211, 30);
            actorEnvironment.makeCacheCrash(l212, 30);
            actorEnvironment.makeCacheCrash(l213, 30);
            actorEnvironment.makeCacheCrash(l214, 30);
            actorEnvironment.makeCacheCrash(l221, 30);
            actorEnvironment.makeCacheCrash(l222, 30);
            actorEnvironment.makeCacheCrash(l223, 30);
            actorEnvironment.makeCacheCrash(l224, 30);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);*/

            /*
            WRITE A VALUE WITH CRASHED L1
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            actorEnvironment.makeClientRead(secondClient, l221, 3);
            actorEnvironment.makeCacheCrash(l221, 10);
            actorEnvironment.makeCacheCrash(l11, 10);
            sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(secondClient, l211, 3);*/

            /*
            CRITREAD A VALUE, THEN READ
             */
            /*actorEnvironment.makeClientCritRead(firstClient, l211, 3);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 3);*/

            /*
            CRITWRITE, THEN READ FROM MULTIPLE L2s
             */
            actorEnvironment.makeClientCritWrite(firstClient, l211, 3, 77);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(secondClient, l211, 3);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(secondClient, l221, 3);

            /*
            CRITWRITE, AFTERWARD NORMAL WRITE TO SAME L2 FOR SAME KEY
             */
            /*actorEnvironment.makeClientCritWrite(firstClient, l211, 3, 77);
            Thread.sleep(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 160);*/

            /*
            CRITWRITE, AFTERWARDS CRITWRITE TO OTHER L2 WITH DIFFERENT L1 FOR SAME KEY
             */
            /*actorEnvironment.makeClientCritWrite(firstClient, l211, 3, 77);
            Thread.sleep(2000);
            actorEnvironment.makeClientCritWrite(firstClient, l221, 3, 160);*/

        } catch (NoSuchElementException exc) {
            System.out.println("Some actor is not available");
        } catch (InterruptedException exc) {
            System.exit(1);
        }

        // Extra long sleep to make sure no message is still around
        Thread.sleep(150000);
        System.exit(0);
    }

}
