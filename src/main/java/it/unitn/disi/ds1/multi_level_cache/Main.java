package it.unitn.disi.ds1.multi_level_cache;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.environment.ActorEnvironment;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheBehaviourConfig;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageConfig;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;

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
        Logger.printHeader();

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

            /*==============================
             READ
             ==============================*/

            /*
            READ A VALUE
             */
            //actorEnvironment.makeClientRead(firstClient, l211, 9);

            /*
            READ UNKNOWN KEY
             */
            //actorEnvironment.makeClientRead(firstClient, l211, 989898989);

            /*
            READ A VALUE WITH CRASHED L2
             */
            // TODO CLIENT RETRY
            /*actorEnvironment.makeCacheCrash(l221, 10000);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l221, 1);*/

            /*
            READ A VALUE WITH CRASHED L1
             */
            /*actorEnvironment.makeCacheCrash(l12, 10000);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l221, 9);*/

            /*
            READ SAME KEY AT THE SAME TIME
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9);
            actorEnvironment.makeClientRead(firstClient, l211, 9);*/

            /*
            READ SAME KEY AT THE SAME TIME AT THE SAME L2 WITH DIFFERENT CLIENTS
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9);
            actorEnvironment.makeClientRead(secondClient, l211, 9);*/

            /*
            READ SAME KEY AT THE SAME TIME AT THE SAME L1 WITH DIFFERENT CLIENTS
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9);
            actorEnvironment.makeClientRead(secondClient, l212, 9);*/

            /*
            READ TWO TIMES AT THE SAME L2 WITH DIFFERENT CLIENTS
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(secondClient, l211, 9);*/

            /*
            READ TWO TIMES AT THE SAME L1 WITH DIFFERENT CLIENTS
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(secondClient, l212, 9);*/

            /*
            READ SAME KEY AT THE SAME TIME
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9);
            actorEnvironment.makeClientRead(firstClient, l211, 9);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 9);
            actorEnvironment.makeClientRead(firstClient, l211, 20);
            actorEnvironment.makeClientRead(secondClient, l211, 20);*/

            /*
            READ AND CRASH L1 AFTERWARDS
             */
            /*actorEnvironment.makeClientRead(
                    firstClient,
                    l211,
                    9,
                    MessageConfig.of(
                            CacheBehaviourConfig.crashAndRecoverAfter(10000),
                            CacheBehaviourConfig.none()
                    )
            );*/

            /*
            READ AND CRASH L2 AFTERWARDS
             */
            /*actorEnvironment.makeClientRead(
                    firstClient,
                    l211,
                    9,
                    MessageConfig.of(
                            CacheBehaviourConfig.none(),
                            CacheBehaviourConfig.crashAndRecoverAfter(10000)
                    )
            );*/

            /*
            READ A VALUE TWICE FROM SAME L2
            */
            /*actorEnvironment.makeClientRead(firstClient, l211, 5);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(secondClient, l211, 5);*/

            /*
            READ A VALUE TWICE FROM DIFFERENT L2 BUT SAME L1
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 5);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(secondClient, l212, 5);*/

            /*==============================
             WRITE
             ==============================*/

            /*
            WRITE A VALUE
             */
            //actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);

            /*
            WRITE A NON-EXISTING VALUE
             */
            //actorEnvironment.makeClientWrite(firstClient, l211, 989898989, 100);

            /*
            READ THEN WRITE AFTERWARDS TO DIFFERENT L1, THEN READ AGAIN FROM SAME L2
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 3);
            Thread.sleep(2000);
            actorEnvironment.makeClientWrite(firstClient, l221, 3, 100);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(secondClient, l211, 3);*/

            /*
            READ THEN WRITE AFTERWARDS TO DIFFERENT L1, THEN READ AGAIN FROM SAME L1
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 3);
            Thread.sleep(2000);
            actorEnvironment.makeClientWrite(firstClient, l221, 3, 100);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(secondClient, l212, 3);*/

            /*
            WRITE WITH CRASHED L2
             */
            /*actorEnvironment.makeCacheCrash(l211, 10000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);*/

            /*
            WRITE WITH CRASHED L1
             */
            /*actorEnvironment.makeCacheCrash(l11, 10000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);*/

            /*
            WRITE TWO TIMES AT THE SAME CLIENT
             */
            /*actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            actorEnvironment.makeClientWrite(firstClient, l211, 5, 333);
            Thread.sleep(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 5, 333);*/

            /*
            WRITE AND CRASH L2 AFTERWARDS
             */
            /*actorEnvironment.makeClientWrite(
                    firstClient,
                    l211,
                    3,
                    100,
                    MessageConfig.of(
                            CacheBehaviourConfig.none(),
                            CacheBehaviourConfig.crashAndRecoverAfter(10000)
                    )
            );*/

            /*
            WRITE AND CRASH L1 AFTERWARDS
             */
            // TODO DO NOT PROPAGATE TO DB WHEN L1 CRASHES (SAME AS ABOVE)
            /*actorEnvironment.makeClientWrite(
                    firstClient,
                    l211,
                    3,
                    100,
                    MessageConfig.of(
                            CacheBehaviourConfig.crashAndRecoverAfter(10000),
                            CacheBehaviourConfig.none()
                    )
            );*/

            /*
            WRITE A LOCKED KEY AT L2
             */
            //actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            //actorEnvironment.makeClientWrite(secondClient, l211, 3, 500);

            /*
            WRITE A LOCKED KEY AT L1
             */
            /*actorEnvironment.makeClientWrite(firstClient, l211, 3, 100);
            actorEnvironment.makeClientWrite(secondClient, l212,3, 500);*/
            /*
            // TODO WRITE WITH THE SAME KEY IS NOT UNIQUELY IDENTIFIABLE (SECOND WRITE BEING CONFIRMED AFTER FIRST REFILL ARRIVES)
            actorEnvironment.makeClientWrite(
                    secondClient,
                    l212,
                    3,
                    500,
                    MessageConfig.of(
                            CacheBehaviourConfig.none(),
                            CacheBehaviourConfig.delayMessage(1)
                    )
            );*/

            /*==============================
             CRITREAD
             ==============================*/

            /*
            CRITREAD A VALUE
             */
            /*actorEnvironment.makeClientRead(firstClient, l211, 9);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(secondClient, l211, 9);
            actorEnvironment.makeClientCritRead(firstClient, l211, 9);*/

            /*
            CRITREAD UNKNOWN KEY
             */
            //actorEnvironment.makeClientCritRead(firstClient, l211, 989898989);

            /*
            CRITREAD A VALUE WITH CRASHED L2
             */
            /*actorEnvironment.makeCacheCrash(l221, 10000);
            actorEnvironment.makeClientCritRead(firstClient, l221, 1);*/

            /*
            CRITREAD A VALUE WITH CRASHED L1
             */
            /*actorEnvironment.makeCacheCrash(l12, 10000);
            actorEnvironment.makeClientCritRead(firstClient, l221, 9);*/

            /*
            CRITREAD SAME KEY AT THE SAME TIME
             */
            /*actorEnvironment.makeClientCritRead(firstClient, l211, 9);
            actorEnvironment.makeClientCritRead(firstClient, l211, 9);
            actorEnvironment.makeClientCritRead(firstClient, l211, 20);*/

            /*
            CRITREAD SAME KEY AT THE SAME TIME AT THE SAME L2 WITH DIFFERENT CLIENTS
             */
            /*actorEnvironment.makeClientCritRead(firstClient, l211, 9);
            actorEnvironment.makeClientCritRead(secondClient, l211, 9);*/

            /*
            CRITREAD SAME KEY AT THE SAME TIME AT THE SAME L1 WITH DIFFERENT CLIENTS
             */
            /*actorEnvironment.makeClientCritRead(firstClient, l211, 9);
            actorEnvironment.makeClientCritRead(secondClient, l212, 9);*/

            /*
            CRITREAD TWO TIMES AT THE SAME L2 WITH DIFFERENT CLIENTS AND DIFFERENT KEYS
             */
            /*actorEnvironment.makeClientCritRead(firstClient, l211, 9);
            actorEnvironment.makeClientCritRead(secondClient, l211, 7);*/

            /*
            CRITREAD TWO TIMES AT THE SAME L1 WITH DIFFERENT CLIENTS AND DIFFERENT KEYS
             */
            /*actorEnvironment.makeClientCritRead(firstClient, l211, 9);
            actorEnvironment.makeClientCritRead(secondClient, l212, 7);*/

            /*
            CRITREAD AND CRASH L1 AFTERWARDS
             */
            /*actorEnvironment.makeClientCritRead(
                    firstClient,
                    l211,
                    9,
                    MessageConfig.of(
                            CacheBehaviourConfig.crashAndRecoverAfter(10000),
                            CacheBehaviourConfig.none()
                    )
            );*/

            /*
            CRITREAD AND CRASH L2 AFTERWARDS
             */
            /*actorEnvironment.makeClientCritRead(
                    firstClient,
                    l211,
                    9,
                    MessageConfig.of(
                            CacheBehaviourConfig.none(),
                            CacheBehaviourConfig.crashAndRecoverAfter(10000)
                    )
            );*/

            /*
            CRITREAD A VALUE TWICE FROM SAME L2
            */
            /*actorEnvironment.makeClientCritRead(firstClient, l211, 5);
            Thread.sleep(2000);
            actorEnvironment.makeClientCritRead(secondClient, l211, 5);*/

            /*
            CRITREAD A VALUE TWICE FROM DIFFERENT L2 BUT SAME L1
             */
            /*actorEnvironment.makeClientCritRead(firstClient, l211, 5);
            Thread.sleep(2000);
            actorEnvironment.makeClientCritRead(secondClient, l212, 5);*/

            /*==============================
             CRITWRITE
             ==============================*/

            /*
            CRITWRITE A KEY
             */
            /*actorEnvironment.makeClientCritWrite(firstClient, l211, 3, 333);
            Thread.sleep(2000);
            actorEnvironment.makeClientRead(firstClient, l211, 3);
            actorEnvironment.makeClientCritRead(secondClient, l223, 3);
            Thread.sleep(2000);
            actorEnvironment.makeClientCritRead(secondClient, l211, 3);*/

            /*
            CRITWRITE AFTERWARDS NORMAL WRITE TO SAME L2 FOR SAME KEY
             */
            /*actorEnvironment.makeClientCritWrite(firstClient, l211, 3, 77);
            Thread.sleep(2000);
            actorEnvironment.makeClientWrite(firstClient, l211, 3, 160);
            Thread.sleep(2000);
            actorEnvironment.makeClientCritRead(secondClient, l211, 3);
            actorEnvironment.makeClientCritRead(secondClient, l223, 3);
            actorEnvironment.makeClientCritRead(secondClient, l211, 3);*/

            /*
            CRITWRITE WITH CRASHED L1
             */
            /*actorEnvironment.makeCacheCrash(l11, 10000);
            actorEnvironment.makeClientCritWrite(firstClient, l211, 3, 77);*/

            /*
            CRITWRITE WITH DIFFERENT CRASHED L1
             */
            /*actorEnvironment.makeCacheCrash(l12, 10000);
            actorEnvironment.makeClientCritWrite(firstClient, l211, 3, 77);*/

            /*
            CRITWRITE WITH CRASHED L2
             */
            /*actorEnvironment.makeCacheCrash(l211, 10000);
            actorEnvironment.makeClientCritWrite(firstClient, l211, 3, 77);*/

            /*
            CRITWRITE AND CRASH L1 AFTERWARDS
             */
            /*actorEnvironment.makeClientCritWrite(
                firstClient,
                l211,
                3,
                77,
                MessageConfig.of(
                        CacheBehaviourConfig.crashAndRecoverAfter(20000),
                        CacheBehaviourConfig.none()
                )
            );*/

            /*
            CRITWRITE AND CRASH L2 AFTERWARDS
             */
            /*actorEnvironment.makeClientCritWrite(
                firstClient,
                l211,
                3,
                77,
                MessageConfig.of(
                        CacheBehaviourConfig.none(),
                        CacheBehaviourConfig.crashAndRecoverAfter(10000)
                )
            );*/

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
