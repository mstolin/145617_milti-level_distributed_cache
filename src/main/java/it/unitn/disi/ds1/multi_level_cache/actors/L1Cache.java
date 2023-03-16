package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public class L1Cache extends Cache {

    private boolean hasRequestedCritWrite = false;
    private int critWriteVotingsCount = 0;

    public L1Cache(String id) {
        super(id, false);
    }

    static public Props props(String id) {
        return Props.create(Cache.class, () -> new L1Cache(id));
    }

    private void resetCritWriteConfig() {
        this.hasRequestedCritWrite = false;
        this.critWriteVotingsCount = 0;
    }

    @Override
    protected void onTimeoutMessage(TimeoutMessage message) {
        if (message.getType() == TimeoutType.CRIT_WRITE_REQUEST && this.hasRequestedCritWrite) {
            // Some L2 has aborted so lets timeout as well
            this.resetCritWriteConfig();
        }
    }

    @Override
    protected void forwardMessageToNext(Serializable message, TimeoutType timeoutType) {
        this.database.tell(message, this.getSelf());
    }

    @Override
    protected void onCritWriteRequestMessage(CritWriteRequestMessage message) {
        int key = message.getKey();
        boolean isOk = !this.data.isLocked(key);

        // if everything isOk, forward to L2s, otherwise force timeout
        if (isOk) {
            // forward to L2s
            this.multicast(message, this.l2Caches);
            this.setMulticastTimeout(message, TimeoutType.CRIT_WRITE_REQUEST);
            this.hasRequestedCritWrite = true;
        }
    }

    @Override
    protected void onCritWriteVoteMessage(CritWriteVoteMessage message) {
        if (!message.isOk()) {
            // some L2 as aborted, abort as well and force timeout
            this.resetCritWriteConfig();
            return;
        }

        // increase the count
        this.critWriteVotingsCount = this.critWriteVotingsCount + 1;
        if (this.critWriteVotingsCount == this.l2Caches.size()) {
            int key = message.getKey();
            // got OK vote from all L2s, lock and answer back to DB
            this.data.lockValueForKey(key);
            CritWriteVoteMessage critWriteVoteMessage = new CritWriteVoteMessage(key, true);
            this.database.tell(critWriteVoteMessage, this.getSelf());
            // reset
            this.resetCritWriteConfig();
        }
    }

    @Override
    protected void onCritWriteAbortMessage(CritWriteAbortMessage message) {
        int key = message.getKey();
        // unlock value
        this.data.unLockValueForKey(key);
        // multicast abort to L2s
        this.multicast(message, this.l2Caches);
    }

    @Override
    protected void onCritWriteCommitMessage(CritWriteCommitMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        int updateCount = message.getUpdateCount();

        // update value
        this.data.setValueForKey(key, value, updateCount);
        // multicast commit to all L2s
        this.multicast(message, this.l2Caches);
        // reset critical write
        this.resetCritWriteConfig();
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
        if (this.isReadUnconfirmed(key)) {
            int value = this.data.getValueForKey(key).get();
            int updateCount = this.data.getUpdateCountForKey(key).get();

            // multicast to L2s who have requested the key
            List<ActorRef> requestedL2s = this.unconfirmedReads.get(key);
            FillMessage fillMessage = new FillMessage(key, value, updateCount);
            this.multicast(fillMessage, requestedL2s);
            // afterwards reset for key
            this.resetReadConfig(key);
        }
    }

    @Override
    protected void recover() {
        super.recover();
        this.resetCritWriteConfig();

        // send flush to all L2s
        System.out.printf("%s - Flush all L2s\n", this.id);
        FlushMessage flushMessage = new FlushMessage(this.getSelf());
        this.multicast(flushMessage, this.l2Caches);
    }
}
