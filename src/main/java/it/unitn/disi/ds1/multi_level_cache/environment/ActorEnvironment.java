package it.unitn.disi.ds1.multi_level_cache.environment;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.multi_level_cache.actors.*;
import it.unitn.disi.ds1.multi_level_cache.messages.*;

import java.util.ArrayList;
import java.util.List;

public class ActorEnvironment {

    private final int numOfL1Caches;
    private final int numOfL2Caches;
    private final int numOfClients;
    private final ActorSystem actorSystem;
    private final ActorRef database;
    private final List<ActorRef> l1Caches;
    private final List<ActorRef> l2Caches;
    private final List<ActorRef> clients;

    public ActorEnvironment(String name, int numOfL1Caches, int numOfL2Caches, int numOfClients) {
        this.numOfL1Caches = numOfL1Caches;
        this.numOfL2Caches = numOfL2Caches;
        this.numOfClients = numOfClients;

        this.actorSystem = ActorSystem.create(name);

        // init actors
        this.database = this.actorSystem.actorOf(Database.props());
        this.l1Caches = this.initL1Caches(numOfL1Caches);
        this.l2Caches = this.initL2Caches(numOfL2Caches, this.l1Caches.size());
        this.clients = this.initClients(numOfClients);

        this.establishCommunication();
    }

    private void establishCommunication() {
        // tell database about all l1 caches
        this.sendJoinL1CachesMessage(this.database, this.l1Caches);

        for (int i = 0; i < this.l1Caches.size(); i++) {
            ActorRef l1Cache = this.l1Caches.get(i);
            // tell l1 about db
            this.sendJoinDatabaseMessage(l1Cache);

            // get l2s
            int startOfL2s = i * numOfL2Caches;
            int endOfL2s = startOfL2s + numOfL2Caches;
            List<ActorRef> l2CachesForL1 = this.l2Caches.subList(startOfL2s, endOfL2s);
            // tell l1 about l2s
            this.sendJoinL2CachesMessage(l1Cache, l2CachesForL1);

            for (ActorRef l2Cache: l2CachesForL1) {
                // tell l2 about l1
                this.sendJoinMainL1CacheMessage(l2Cache, l1Cache);
                // tell l2 about the db
                this.sendJoinDatabaseMessage(l2Cache);
                // tell l2 about other l1 caches
                List<ActorRef> otherL1Caches = this.l1Caches.stream().filter((l1) -> l1 != l1Cache).toList();
                this.sendJoinL1CachesMessage(l2Cache, otherL1Caches); // todo IS THIS NECESSAR?, CHECK IF L@ SENDS TO DB ON TIMEOUT
            }
        }

        // tell client about l2 caches
        for (ActorRef client: this.clients) {
            this.sendJoinL2CachesMessage(client, this.l2Caches);
        }
    }

    public void sendJoinDatabaseMessage(ActorRef actor) {
        JoinDatabaseMessage message = new JoinDatabaseMessage(this.database);
        actor.tell(message, ActorRef.noSender());
    }

    public void sendJoinMainL1CacheMessage(ActorRef actor, ActorRef mainL1Cache) {
        JoinMainL1CacheMessage message = new JoinMainL1CacheMessage(mainL1Cache);
        actor.tell(message, ActorRef.noSender());
    }

    public void sendJoinL1CachesMessage(ActorRef actor, List<ActorRef> l1Caches) {
        JoinL1CachesMessage message = new JoinL1CachesMessage(l1Caches);
        actor.tell(message, ActorRef.noSender());
    }

    public void sendJoinL2CachesMessage(ActorRef actor, List<ActorRef> l2Caches) {
        JoinL2CachesMessage message = new JoinL2CachesMessage(l2Caches);
        actor.tell(message, ActorRef.noSender());
    }

    private List<ActorRef> initL1Caches(int total) {
        List<ActorRef> actors = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            String id = this.genL1Id(i+1);
            ActorRef actor = this.actorSystem.actorOf(Cache.props(id, false));
            actors.add(actor);
        }
        return List.copyOf(actors);
    }

    private List<ActorRef> initL2Caches(int total, int l1Size) {
        List<ActorRef> actors = new ArrayList<>();
        for (int i = 0; i < l1Size; i++) {
            for (int j = 0; j < total; j++) {
                String id = this.genL2Id(i+1, j+1);
                ActorRef actor = this.actorSystem.actorOf(Cache.props(id, true));
                actors.add(actor);
            }
        }
        return List.copyOf(actors);
    }

    private List<ActorRef> initClients(int total) {
        List<ActorRef> actors = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            String id = this.genClientId(i+1);
            ActorRef actor = this.actorSystem.actorOf(Client.props(id));
            actors.add(actor);
        }
        return List.copyOf(actors);
    }

    private String genL1Id(int id) {
        return String.format("L1-%d", id);
    }

    private String genL2Id(int l1Id, int l2Id) {
        return String.format("L2-%d-%d", l1Id, l2Id);
    }

    private String genClientId(int id) {
        return String.format("Client-%d", id);
    }

    /*private void sendJoinActorMessage(ActorRef receiver, ActorRef actorToJoin) {
        JoinActorMessage joinActorMessage = new JoinActorMessage(actorToJoin);
        receiver.tell(joinActorMessage, null);
    }

    private void sendJoinGroupMessage(ActorRef receiver, List<ActorRef> groupToJoin) {
        JoinGroupMessage joinGroupMessage = new JoinGroupMessage(groupToJoin);
        receiver.tell(joinGroupMessage, null);
    }*/

    public ActorRef getDatabase() {
        return database;
    }

    public List<ActorRef> getL1Caches() {
        return l1Caches;
    }

    public List<ActorRef> getL2Caches() {
        return l2Caches;
    }

    public List<ActorRef> getClients() {
        return clients;
    }

}
