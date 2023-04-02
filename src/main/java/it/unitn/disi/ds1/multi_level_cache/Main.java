package it.unitn.disi.ds1.multi_level_cache;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.environment.ActorEnvironment;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;

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

        /*
        TODO
        - Error message from L2 (Key unknown error message)
        - Cache on read also check if write is unconfirmed for key
        - No lock on (crit-)read
        - On critread doe L2 forward to DB on crash?
        - L1 cache lock on CritWriteVoteMessage
        - Database lock on CritWrite message
        - Send error message to client, when aborting during critwrite
         */

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
            actorEnvironment.makeClientRead(firstClient, l211, 9);*/

            /*
            READ A VALUE, MAKE L1 CRASH AFTER RECEIVED
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9,
                    CacheCrashConfig.create(0, 10000), CacheCrashConfig.empty());*/

            /*
            READ UNKNOWN KEY
             */
            actorEnvironment.makeClientRead(firstClient, l211, 989898989);

            /*
            READ, TWO DIFFERENT CLIENTS AT THE SAME TIME, SAME L2
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9);
            actorEnvironment.makeClientRead(secondClient, l211, 9);*/

            /*
            READ DIFFERENT VALUE, TWO DIFFERENT CLIENTS AT THE SAME TIME, SAME L2
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9);
            actorEnvironment.makeClientRead(secondClient, l211, 3);*/

            /*
            READ, TWO DIFFERENT CLIENTS AT THE SAME TIME, DIFFERENT L2, SAME L1
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9);
            actorEnvironment.makeClientRead(secondClient, l212, 9);*/

            /*
            READ, TWO DIFFERENT CLIENTS AT THE SAME TIME, DIFFERENT L2, DIFFERENT L1
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9);
            actorEnvironment.makeClientRead(secondClient, l221, 9);*/

            /*
            READ A VALUE FROM SAME L2
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 3);
            sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 3);*/

            /*
            READ A DIFFERENT VALUE FROM TWICE FROM DIFFERENT L2 BUT SAME L1
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 5);
            sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l212, 5);*/

            /*
            READ A VALUE WITH CRASHED L1
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeCacheCrash(l12, 5000);
            sleepAndDelimiter(2000);
            actorEnvironment.makeClientRead(firstClient, l221, 9);*/

            /*
            READ A VALUE WITH CRASHED L2
             */
            /*Thread.sleep(2000);
            actorEnvironment.makeCacheCrash(l221, 10000);
            actorEnvironment.makeClientRead(firstClient, l221, 1);*/

            /*
            WRITE A VALUE
             */
            //actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);

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
            actorEnvironment.makeCacheCrash(l211, 10000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);*/

            /*
            WRITE A VALUE WITH ONLY CRASHED L2
             */
            /*actorEnvironment.makeCacheCrash(l211, 30000);
            actorEnvironment.makeCacheCrash(l212, 30000);
            actorEnvironment.makeCacheCrash(l213, 30000);
            actorEnvironment.makeCacheCrash(l214, 30000);
            actorEnvironment.makeCacheCrash(l221, 30000);
            actorEnvironment.makeCacheCrash(l222, 30000);
            actorEnvironment.makeCacheCrash(l223, 30000);
            actorEnvironment.makeCacheCrash(l224, 30000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);*/

            /*
            WRITE A VALUE WITH CRASHED L1
             */
            /*sleepAndDelimiter(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            actorEnvironment.makeClientRead(secondClient, l221, 3);
            actorEnvironment.makeCacheCrash(l221, 10000);
            actorEnvironment.makeCacheCrash(l11, 10000);
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
            /*actorEnvironment.makeClientCritWrite(firstClient, l211, 3, 77);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(secondClient, l211, 3);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(secondClient, l221, 3);*/

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

            /*
            CRITWRITE, WITH CRASHED L1
             */
            /*actorEnvironment.makeCacheCrash(l11, 10000);
            actorEnvironment.makeClientCritWrite(firstClient, l211, 3, 77);*/

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
