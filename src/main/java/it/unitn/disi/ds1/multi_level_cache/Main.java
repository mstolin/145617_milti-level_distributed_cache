package it.unitn.disi.ds1.multi_level_cache;

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

        // send random write message
        actorEnvironment.makeClientWrite(0, 3, 100);

        // send random read message
        actorEnvironment.makeClientReadAfter(10, 1, 3);

        // todo Implement:
        // Better to have some other kind of class that handles actions, then only call:
        // ActorRef a = actorEnvironment.getRandomClient()
        // dispatcher.sentReadMessage(a)
        // ...
    }

}
