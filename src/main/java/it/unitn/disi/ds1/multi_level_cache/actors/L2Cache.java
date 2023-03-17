package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

import java.io.Serializable;
import java.util.List;

public class L2Cache extends Cache {

    public L2Cache(String id) {
        super(id);
    }

    static public Props props(String id) {
        return Props.create(Cache.class, () -> new L2Cache(id));
    }

    @Override
    protected void forwardMessageToNext(Serializable message, TimeoutType timeoutType) {
        this.mainL1Cache.tell(message, this.getSelf());
        this.setTimeout(message, this.mainL1Cache, timeoutType);
    }

    @Override
    protected void handleWriteMessage(WriteMessage message) {
        // add client to lust
        this.addUnconfirmedWrite(message.getKey(), this.getSender());
        // forward to L1
        this.forwardMessageToNext(message, TimeoutType.WRITE);
    }

    @Override
    protected void handleRefillMessage(RefillMessage message) {
        int key = message.getKey();
        if (this.isWriteUnconfirmed(key)) {
            // tell client write confirm
            ActorRef client = this.unconfirmedWrites.get(key);
            int value = message.getValue();
            int updateCount = message.getUpdateCount();
            WriteConfirmMessage confirmMessage = new WriteConfirmMessage(key, value, updateCount);
            client.tell(confirmMessage, this.getSelf());
            // reset timeout
            this.resetWriteConfig(key);
        }
    }

    @Override
    protected void handleTimeoutMessage(TimeoutMessage message) {
        // forward message to DB, no need for timeout since DB can't timeout
        /*
        TODO Was ist wenn DB timeout wegen ein lock?
         */
        if (message.getType() == TimeoutType.READ) {
            ReadMessage readMessage = (ReadMessage) message.getMessage();
            int key = readMessage.getKey();

            // if the key is in this map, then no ReadReply has been received for the key
            if (this.isReadUnconfirmed(key)) {
                System.out.printf("%s - has timed out for read, forward message directly to DB\n", this.id);
                this.database.tell(readMessage, this.getSelf());
            }
        } else if (message.getType() == TimeoutType.CRIT_READ) {
            CritReadMessage critReadMessage = (CritReadMessage) message.getMessage();
            int key = critReadMessage.getKey();

            // if the key is in this map, then no ReadReply has been received for the key
            if (this.isReadUnconfirmed(key)) {
                System.out.printf("%s - has timed out for crit read, forward message directly to DB\n", this.id);
                this.database.tell(critReadMessage, this.getSelf());
            }
        } else if (message.getType() == TimeoutType.WRITE) {
            WriteMessage writeMessage = (WriteMessage) message.getMessage();
            int key = writeMessage.getKey();

            if (this.isWriteUnconfirmed(key)) {
                System.out.printf("%s - has timed out for write, forward message directly to DB\n", this.id);
                this.database.tell(writeMessage, this.getSelf());
            }
        }
    }

    @Override
    protected void handleCritWriteMessage(CritWriteMessage message) {
        /*
        nothing special todo here, all important happening in Cache
         */
    }

    @Override
    protected void handleCritWriteRequestMessage(CritWriteRequestMessage message, boolean isOk) {
        int key = message.getKey();
        if (isOk) {
            // just lock data
            this.data.lockValueForKey(key);
        }
        // answer back
        CritWriteVoteMessage critWriteVoteOkMessage = new CritWriteVoteMessage(key, isOk);
        this.mainL1Cache.tell(critWriteVoteOkMessage, this.getSelf());
    }

    @Override
    protected void handleCritWriteVoteMessage(CritWriteVoteMessage message) {
        /*
        Do nothing here, L2 only sends vote messages
         */
    }

    @Override
    protected void handleCritWriteAbortMessage(CritWriteAbortMessage message) {
        this.resetWriteConfig(message.getKey());
    }

    @Override
    protected void handleCritWriteCommitMessage(CritWriteCommitMessage message) {
        // just update the value
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();

        // response to client if needed
        if (this.isWriteUnconfirmed(key)) {
            ActorRef client = this.unconfirmedWrites.get(key);
            WriteConfirmMessage confirmMessage = new WriteConfirmMessage(key, value, updateCount);
            client.tell(confirmMessage, this.getSelf());
        }

        // reset critical write
        this.resetWriteConfig(key);
    }

    @Override
    protected void multicastReFillMessageIfNeeded(int key, int value, int updateCount, ActorRef sender) {
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
    protected void handleFill(int key) {
        if (this.isReadUnconfirmed(key)) {
            int value = this.data.getValueForKey(key).get();
            int updateCount = this.data.getUpdateCountForKey(key).get();

            // multicast to clients who have requested the key
            List<ActorRef> clients = this.unconfirmedReads.get(key);
            ReadReplyMessage readReplyMessage = new ReadReplyMessage(key, value, updateCount);
            this.multicast(readReplyMessage, clients);
            // reset for key
            this.resetReadConfig(key);
        }
    }
}
