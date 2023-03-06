package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.ReadReplyMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.TimeoutMessage;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Optional;

public class L2Cache extends Cache {

    public L2Cache(String id) {
        super(id, true);
    }

    static public Props props(String id) {
        return Props.create(Cache.class, () -> new L2Cache(id));
    }

    @Override
    protected void onTimeoutMessage(TimeoutMessage message) {
        System.out.printf("%s - has timed out, forward message directly to DB\n");
        this.database.tell(message, this.getSelf());
    }

    @Override
    protected void forwardMessageToNext(Serializable message, TimeoutType timeoutType) {
        this.mainL1Cache.tell(message, this.getSelf());
        this.setTimeout(message, this.mainL1Cache, timeoutType);
    }

    @Override
    protected void multicastReFillMessage(int key, int value, int updateCount, ActorRef sender) {
        /*
        No need to implement. L2 cache does not send ReFill message.
         */
    }

    /**
     * Sends a ReadReply message to the saved sender. A ReadReply message is only send
     * back to the client. Therefore, no need to start a timeout, since a client is
     * not supposed to crash.
     *
     * @param key The key received by the ReadMessage
     */
    @Override
    protected void responseForFillOrReadReply(int key) {
        Optional<Integer> value = this.data.getValueForKey(key);
        Optional<Integer> updateCount = this.data.getUpdateCountForKey(key);

        if (this.currentReadMessages.containsKey(key) && value.isPresent() && updateCount.isPresent()) {
            // get client
            ActorRef client = this.currentReadMessages.get(key);
            this.currentReadMessages.remove(key);
            // send message
            ReadReplyMessage readReplyMessage = new ReadReplyMessage(key, value.get(), updateCount.get());
            client.tell(readReplyMessage, this.getSelf());
        }
    }
}
