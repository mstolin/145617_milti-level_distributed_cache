package it.unitn.disi.ds1.multi_level_cache;

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

    public static void main(String[] args) {
        ActorEnvironment actorEnvironment = new ActorEnvironment(
                "Multi-Level-Cache", numberOfL1Caches, numberOfL2Caches, numberOfClients);

        ClientWrapper firstClient = new ClientWrapper(actorEnvironment.getClients().get(0), actorEnvironment.getL2Caches());
        ClientWrapper secondClient = new ClientWrapper(actorEnvironment.getClients().get(1), actorEnvironment.getL2Caches());
        firstClient.sendReadMessage(3);
        secondClient.sendWriteMessage(3, 100);
    }

}
