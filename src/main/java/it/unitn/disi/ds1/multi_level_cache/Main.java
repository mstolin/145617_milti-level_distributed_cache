package it.unitn.disi.ds1.multi_level_cache;

import it.unitn.disi.ds1.multi_level_cache.environment.ActorEnvironment;
import it.unitn.disi.ds1.multi_level_cache.environment.ClientWrapper;

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
    private static final Integer numberOfClients = 4;

    public static void main(String[] args) throws InterruptedException {
        ActorEnvironment actorEnvironment = new ActorEnvironment(
                "Multi-Level-Cache", numberOfL1Caches, numberOfL2Caches, numberOfClients);

        ClientWrapper firstClient = new ClientWrapper(actorEnvironment.getClients().get(0), actorEnvironment.getL2Caches());
        ClientWrapper secondClient = new ClientWrapper(actorEnvironment.getClients().get(1), actorEnvironment.getL2Caches());

        System.out.println("### WRITE KEY 3 ###");
        firstClient.sendWriteMessage(3, 100, 0);
        Thread.sleep(4000);
        System.out.println("### READ KEY 3 ###");
        secondClient.sendReadMessage(3, 5);
        Thread.sleep(4000);
        System.out.println("### READ KEY 3 AGAIN ###");
        secondClient.sendReadMessage(3, 5);

        Thread.sleep(4000);
        System.exit(0);
    }

}
