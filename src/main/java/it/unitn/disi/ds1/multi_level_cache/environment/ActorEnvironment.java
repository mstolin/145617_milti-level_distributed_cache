package it.unitn.disi.ds1.multi_level_cache.environment;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.multi_level_cache.actors.*;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

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
            }
        }

        // tell client about l2 caches
        for (ActorRef client: this.clients) {
            this.sendJoinL2CachesMessage(client, this.l2Caches);
        }
    }

    private void sendJoinDatabaseMessage(ActorRef actor) {
        JoinDatabaseMessage message = new JoinDatabaseMessage(this.database);
        actor.tell(message, ActorRef.noSender());
    }

    private void sendJoinMainL1CacheMessage(ActorRef actor, ActorRef mainL1Cache) {
        JoinMainL1CacheMessage message = new JoinMainL1CacheMessage(mainL1Cache);
        actor.tell(message, ActorRef.noSender());
    }

    private void sendJoinL1CachesMessage(ActorRef actor, List<ActorRef> l1Caches) {
        JoinL1CachesMessage message = new JoinL1CachesMessage(l1Caches);
        actor.tell(message, ActorRef.noSender());
    }

    private void sendJoinL2CachesMessage(ActorRef actor, List<ActorRef> l2Caches) {
        JoinL2CachesMessage message = new JoinL2CachesMessage(l2Caches);
        actor.tell(message, ActorRef.noSender());
    }

    private List<ActorRef> initL1Caches(int total) {
        List<ActorRef> actors = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            String id = this.genL1Id(i+1);
            ActorRef actor = this.actorSystem.actorOf(L1Cache.props(id));
            actors.add(actor);
        }
        return List.copyOf(actors);
    }

    private List<ActorRef> initL2Caches(int total, int l1Size) {
        List<ActorRef> actors = new ArrayList<>();
        for (int i = 0; i < l1Size; i++) {
            for (int j = 0; j < total; j++) {
                String id = this.genL2Id(i+1, j+1);
                ActorRef actor = this.actorSystem.actorOf(L2Cache.props(id));
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

    public ActorSystem getActorSystem() {
        return this.actorSystem;
    }

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

    private Optional<ActorRef> getActorFromList(List<ActorRef> actors, int index) {
        if (index >= actors.size()) {
            return Optional.empty();
        }
        ActorRef actor = actors.get(index);
        return Optional.of(actor);
    }

    public Optional<ActorRef> getClient(int index) {
        return this.getActorFromList(this.clients, index);
    }

    public ActorRef getRandomClient() {
        Random rand = new Random();
        return this.clients.get(rand.nextInt(this.clients.size()));
    }

    public Optional<ActorRef> getL1Cache(int index) {
        return this.getActorFromList(this.l1Caches, index);
    }

    public Optional<ActorRef> getL2Cache(int index) {
        return this.getActorFromList(this.l2Caches, index);
    }

    public void makeClientWrite(ActorRef client, ActorRef l2Cache, int key, int value, MessageConfig messageConfig) {
        InstantiateWriteMessage message = new InstantiateWriteMessage(key, value, l2Cache, false, messageConfig);
        client.tell(message, ActorRef.noSender());
    }

    public void makeClientWrite(ActorRef client, ActorRef l2Cache, int key, int value) {
        this.makeClientWrite(client, l2Cache, key, value, MessageConfig.none());
    }

    public void makeClientCritWrite(ActorRef client, ActorRef l2Cache, int key, int value, MessageConfig messageConfig) {
        InstantiateWriteMessage message = new InstantiateWriteMessage(key, value, l2Cache, true, messageConfig);
        client.tell(message, ActorRef.noSender());
    }

    public void makeClientCritWrite(ActorRef client, ActorRef l2Cache, int key, int value) {
        this.makeClientCritWrite(client, l2Cache, key, value, MessageConfig.none());
    }

    public void makeRandomClientWrite(ActorRef l2Cache, int key, int value) {
        ActorRef randomClient = this.getRandomClient();
        this.makeClientWrite(randomClient, l2Cache, key, value);
    }

    public void makeClientRead(ActorRef client, ActorRef l2Cache, int key, MessageConfig messageConfig) {
        InstantiateReadMessage message = new InstantiateReadMessage(key, l2Cache, false, messageConfig);
        client.tell(message, ActorRef.noSender());
    }

    public void makeClientRead(ActorRef client, ActorRef l2Cache, int key) {
        this.makeClientRead(client, l2Cache, key, MessageConfig.none());
    }

    public void makeClientCritRead(ActorRef client, ActorRef l2Cache, int key, MessageConfig messageConfig) {
        InstantiateReadMessage message = new InstantiateReadMessage(key, l2Cache, true, messageConfig);
        client.tell(message, ActorRef.noSender());
    }

    public void makeClientCritRead(ActorRef client, ActorRef l2Cache, int key) {
        this.makeClientCritRead(client, l2Cache, key, MessageConfig.none());
    }

    public void makeRandomClientRead(ActorRef l2Cache, int key) {
        ActorRef randomClient = this.getRandomClient();
        this.makeClientRead(randomClient, l2Cache, key);
    }

    public void makeCacheCrash(ActorRef cache, long recoverAfter) {
        CrashMessage message = new CrashMessage(recoverAfter);
        cache.tell(message, ActorRef.noSender());
    }

}
