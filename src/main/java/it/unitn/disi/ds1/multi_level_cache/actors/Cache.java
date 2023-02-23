package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.RefillMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteConfirmMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.WriteMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public abstract class Cache extends AbstractActor {

    protected Map<UUID, ActorRef> writeHistory = new HashMap<>();

    public final String id;

    protected Map<Integer, Integer> cache = new HashMap<>();

    public Cache(String id) {
        this.id = id;
    }

    protected abstract void forwardWriteToNext(WriteMessage message);
    protected abstract void forwardConfirmWriteToSender(WriteConfirmMessage message);

    protected abstract void addToWriteHistory(UUID uuid);

    protected void onWriteMessage(WriteMessage message) {
        // just forward message for now
        System.out.printf("%s received write message (%s), forward to next\n", this.id, message.getUuid().toString());

        if (!this.writeHistory.containsKey(message.getUuid())) {
            /*
            Message is not known, so we need to add the message to our history,
            then forward to the next actor (L1 cache or database).
             */
            this.addToWriteHistory(message.getUuid());
            this.forwardWriteToNext(message);
        } else {
            // todo Error this shouldn't be
        }
    }

    protected void onWriteConfirmMessage(WriteConfirmMessage message) {
        System.out.printf(
                "%s received write confirm message (%s), forward to sender\n",
                this.id, message.getWriteMessageUUID().toString());

        if (this.writeHistory.containsKey(message.getWriteMessageUUID())) {
            /*
            Message id is known. First, update cache, forward confirm to sender,
            lastly remove message from our history.
             */
            this.cache.put(message.getKey(), message.getValue());
            this.forwardConfirmWriteToSender(message);
            this.writeHistory.remove(message.getWriteMessageUUID());
        } else {
            // todo Error this shouldn't be
        }
    }

    protected void onRefillMessage(RefillMessage message) {
        System.out.printf(
                "%s received refill message for key %d. Update if needed.\n",
                this.id, message.getKey());

        if (this.cache.containsKey(message.getKey())) {
            // we need to update
            this.cache.put(message.getKey(), message.getValue());
        } else {
            // never known this key, don't update
            System.out.printf("%s never read/write key %d, therefore no update\n", this.id, message.getKey());
        }
    }

}
