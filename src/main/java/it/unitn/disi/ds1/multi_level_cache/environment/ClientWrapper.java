package it.unitn.disi.ds1.multi_level_cache.environment;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.InstantiateReadMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.InstantiateWriteMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.ReadMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteMessage;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Random;

public class ClientWrapper {

    private final ActorRef actor;
    private final List<ActorRef> l2Caches;

    public ClientWrapper(ActorRef actor, List<ActorRef> l2Caches) {
        this.actor = actor;
        this.l2Caches = l2Caches;
    }

    private ActorRef getRandomL2Cache() {
        Random rand = new Random();
        return this.l2Caches.get(rand.nextInt(this.l2Caches.size()));
    }

    private Optional<ActorRef> getL2Cache(int index) {
        try {
            return Optional.of(this.l2Caches.get(index));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private void sendReadMessage(int key, ActorRef l2Cache) {
        InstantiateReadMessage message = new InstantiateReadMessage(key, l2Cache);
        this.actor.tell(message, ActorRef.noSender());
    }

    private void sendWriteMessage(int key, int value, ActorRef l2Cache) {
        InstantiateWriteMessage message = new InstantiateWriteMessage(key, value, l2Cache);
        this.actor.tell(message, ActorRef.noSender());
    }

    public void sendReadMessage(int key, int l2Index) throws NoSuchElementException {
        Optional<ActorRef> l2Cache = this.getL2Cache(l2Index);
        if (l2Cache.isPresent()) {
            this.sendReadMessage(key, l2Cache.orElseThrow());
        }
    }

    public void sendReadMessage(int key) {
        ActorRef l2Cache = this.getRandomL2Cache();
        this.sendReadMessage(key, l2Cache);
    }

    public void sendWriteMessage(int key, int value, int l2Index) throws NoSuchElementException {
        Optional<ActorRef> l2Cache = this.getL2Cache(l2Index);
        if (l2Cache.isPresent()) {
            this.sendWriteMessage(key, value, l2Cache.orElseThrow());
        }
    }

    public void sendWriteMessage(int key, int value) {
        ActorRef l2Cache = this.getRandomL2Cache();
        this.sendWriteMessage(key, value, l2Cache);
    }

}
