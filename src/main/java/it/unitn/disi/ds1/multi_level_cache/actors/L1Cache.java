package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.FillMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.RefillMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.TimeoutMessage;

import java.io.Serializable;
import java.util.Optional;

public class L1Cache extends Cache {

    public L1Cache(String id) {
        super(id, false);
    }

    @Override
    protected void onTimeoutMessage(TimeoutMessage message) {
        /*
        No need to implement. Can only time-out on DB and
        DB can't crash.
         */
    }

    static public Props props(String id) {
        return Props.create(Cache.class, () -> new L1Cache(id));
    }


    @Override
    protected void forwardMessageToNext(Serializable message) {
        this.database.tell(message, this.getSelf());
    }

    @Override
    protected void multicastReFillMessage(int key, int value, int updateCount, ActorRef sender) {
        RefillMessage reFillMessage = new RefillMessage(key, value, updateCount);

        if (sender != ActorRef.noSender()) {
            this.multicast(reFillMessage, this.l2Caches);
        }
        for (ActorRef cache: this.l2Caches) {
            if (cache != sender) {
                cache.tell(reFillMessage, this.getSelf());
            }
        }
    }

    @Override
    protected void responseForFillOrReadReply(int key) {
        // todo here we need to check if l2 has crashed, then read reply directly back to client (need to add client to the msg)
        Optional<Integer> value = this.data.getValueForKey(key);
        Optional<Integer> updateCount = this.data.getUpdateCountForKey(key);

        if (this.currentReadMessages.containsKey(key) && value.isPresent() && updateCount.isPresent()) {
            ActorRef cache = this.currentReadMessages.get(key);
            FillMessage fillMessage = new FillMessage(key, value.get(), updateCount.get());
            cache.tell(fillMessage, this.getSelf());
            this.currentReadMessages.remove(key);
        }
    }
}
