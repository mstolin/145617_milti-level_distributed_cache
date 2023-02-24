package it.unitn.disi.ds1.multi_level_cache;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.disi.ds1.multi_level_cache.actors.Client;
import it.unitn.disi.ds1.multi_level_cache.actors.Database;
import it.unitn.disi.ds1.multi_level_cache.actors.L1Cache;
import it.unitn.disi.ds1.multi_level_cache.actors.L2Cache;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinActorMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.JoinGroupMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.ReadMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteMessage;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ActorEnvironment {

    private final ActorSystem actorSystem;
    private final ActorRef database;
    private final List<ActorRef> l1Caches;
    private final List<ActorRef> l2Caches;
    private final List<ActorRef> clients;

    public ActorEnvironment(String name, int numOfL1Caches, int numOfL2Caches, int numOfClients) {
        this.actorSystem = ActorSystem.create(name);

        this.l1Caches = this.initL1Caches(numOfL1Caches);

        this.database = this.actorSystem.actorOf(Database.props());
        // tell database about all l1 caches
        this.sendJoinGroupMessage(this.database, this.l1Caches);

        List<ActorRef> allL2Caches = new ArrayList<>();
        for (int i = 0; i < this.l1Caches.size(); i++) {
            ActorRef l1Cache = this.l1Caches.get(i);
            // tell l1 cache about database
            this.sendJoinActorMessage(l1Cache, this.database);
            // generate l2 caches for each l1 cache
            List<ActorRef> l2Caches = this.initL2Caches(numOfL2Caches, i);
            for (ActorRef l2Cache: l2Caches) {
                // tell l2 cache about l1 cache
                this.sendJoinActorMessage(l2Cache, l1Cache);
            }
            // tell l1 cache about l2 caches
            this.sendJoinGroupMessage(l1Cache, l2Caches);

            allL2Caches.addAll(l2Caches);
        }
        this.l2Caches = List.copyOf(allL2Caches);

        this.clients = this.initClients(numOfClients);
        for (ActorRef client: this.clients) {
            // tell client about all l2 caches
            this.sendJoinGroupMessage(client, this.l2Caches);
        }
    }

    private List<ActorRef> initL1Caches(int total) {
        List<ActorRef> actors = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            ActorRef actor = this.actorSystem.actorOf(L1Cache.props(i));
            actors.add(actor);
        }
        return List.copyOf(actors);
    }

    private List<ActorRef> initL2Caches(int total, int l1Id) {
        List<ActorRef> actors = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            ActorRef actor = this.actorSystem.actorOf(L2Cache.props(l1Id, i));
            actors.add(actor);
        }
        return List.copyOf(actors);
    }

    private List<ActorRef> initClients(int total) {
        List<ActorRef> actors = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            ActorRef actor = this.actorSystem.actorOf(Client.props(i));
            actors.add(actor);
        }
        return List.copyOf(actors);
    }

    private void sendJoinActorMessage(ActorRef receiver, ActorRef actorToJoin) {
        JoinActorMessage joinActorMessage = new JoinActorMessage(actorToJoin);
        receiver.tell(joinActorMessage, null);
    }

    private void sendJoinGroupMessage(ActorRef receiver, List<ActorRef> groupToJoin) {
        JoinGroupMessage joinGroupMessage = new JoinGroupMessage(groupToJoin);
        receiver.tell(joinGroupMessage, null);
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

    public void makeClientRead(int clientIndex, int key) {
        if (clientIndex > this.clients.size() - 1) {
            // client doesn't exist
            return;
        }
        ReadMessage readMessage = new ReadMessage(key);
        ActorRef client = this.clients.get(clientIndex);
        client.tell(readMessage, ActorRef.noSender());
    }

    public void makeClientReadAfter(long seconds, int clientIndex, int key) {
        if (clientIndex > this.clients.size() - 1) {
            // client doesn't exist
            return;
        }

        ReadMessage readMessage = new ReadMessage(key);
        ActorRef client = this.clients.get(clientIndex);

        this.actorSystem.getScheduler().scheduleOnce(
                Duration.ofSeconds(seconds),
                client,
                readMessage,
                this.actorSystem.dispatcher(),
                ActorRef.noSender()
        );
    }

    public void makeClientWrite(int clientIndex, int key, int value) {
        if (clientIndex > this.clients.size() - 1) {
            // client doesn't exist
            return;
        }
        WriteMessage writeMessage = new WriteMessage(key, value);
        ActorRef client = this.clients.get(clientIndex);
        client.tell(writeMessage, ActorRef.noSender());
    }

}
