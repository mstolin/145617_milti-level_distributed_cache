package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

import java.util.List;
import java.util.Optional;
import java.util.Random;

public class Database extends Node {

    private List<ActorRef> l1Caches;
    private List<ActorRef> l2Caches;
    private boolean hasRequestedCritWrite = false;
    private int critWriteVotingCount = 0;
    private Optional<Integer> critWriteKey = Optional.empty();
    private Optional<Integer> critWriteValue = Optional.empty();

    public Database() {
        super("Database");
        this.setDefaultData(10);
    }

    static public Props props() {
        return Props.create(Database.class, () -> new Database());
    }

    private void setDefaultData(int size) {
        for (int i = 0; i < size; i++) {
            int value = new Random().nextInt(1000);
            this.data.setValueForKey(i, value, 1);
        }
    }

    private void readAndResponseFill(int key, ActorRef sender) {
        Optional<Integer> value = this.data.getValueForKey(key);
        Optional<Integer> updateCount = this.data.getUpdateCountForKey(key);
        if (value.isPresent() && updateCount.isPresent()) {
            System.out.printf("Database - Requested value is %d, send fill message to sender\n", value.get());
            FillMessage fillMessage = new FillMessage(key, value.get(), updateCount.get());
            sender.tell(fillMessage, this.getSelf());
        } else {
            System.out.printf("Database does not know about key %d\n", key);
            // todo send error response
        }
    }

    private void resetCritWriteConfig() {
        this.critWriteKey = Optional.empty();
        this.critWriteValue = Optional.empty();
        this.hasRequestedCritWrite = false;
        this.critWriteVotingCount = 0;
    }

    private void abortCritWrite() {
        int key = this.critWriteKey.get();
        // unlock
        this.data.unLockValueForKey(key);
        // multicast abort message
        CritWriteAbortMessage abortMessage = new CritWriteAbortMessage(key);
        this.multicast(abortMessage, this.l1Caches);
        // reset
        this.resetCritWriteConfig();
    }

    private void onJoinL1Caches(JoinL1CachesMessage message) {
        this.l1Caches = List.copyOf(message.getL1Caches());
        System.out.printf("Database joined group of %d L1 caches\n", this.l1Caches.size());
    }

    private void onJoinL2Caches(JoinL2CachesMessage message) {
        this.l2Caches = List.copyOf(message.getL2Caches());
        System.out.printf("Database joined group of %d L2 caches\n", this.l2Caches.size());
    }

    private void onWriteMessage(WriteMessage message) {
        int key = message.getKey();
        int value = message.getValue();;
        System.out.printf("Database - Received write message of {%d: %d}\n", key, value);

        if (this.data.isLocked(key)) {
            System.out.printf("Database - Can't write key %d is locked\n", key);
            return; // todo send error
        }

        // Lock data
        this.data.lockValueForKey(key);

        // Write data
        this.data.setValueForKey(key, value);
        // we can be sure it exists, since we set value previously
        int updateCount = this.data.getUpdateCountForKey(key).get();

        // Send confirm to L1 sender
        // todo make own method
        WriteConfirmMessage writeConfirmMessage = new WriteConfirmMessage(
                message.getUuid(), message.getKey(), message.getValue(), updateCount);
        this.getSender().tell(writeConfirmMessage, this.getSelf());

        // Send refill to all other L1 caches
        // todo make own method
        RefillMessage refillMessage = new RefillMessage(message.getKey(), message.getValue(), updateCount);
        for (ActorRef l1Cache: this.l1Caches) {
            if (l1Cache != this.getSender()) {
                l1Cache.tell(refillMessage, this.getSelf());
            }
        }

        // Unlock value
        this.data.unLockValueForKey(key);
    }

    private void onCritWriteMessage(CritWriteMessage message) {
        int key = message.getKey();
        int value = message.getValue();;
        System.out.printf("Database - Received critical write message of {%d: %d}\n", key, value);

        if (this.data.isLocked(key)) {
            System.out.printf("Database - Can't write key %d is locked\n", key);
            return; // todo send error
        }

        // lock value
        this.data.lockValueForKey(key);

        // Multicast vote request to all L1s // todo make own method
        CritWriteRequestMessage critWriteRequestMessage = new CritWriteRequestMessage(key);
        this.multicast(critWriteRequestMessage, this.l1Caches);
        this.setMulticastTimeout(critWriteRequestMessage, TimeoutType.CRIT_WRITE_REQUEST);
        // set crit write config
        this.critWriteKey = Optional.of(key);
        this.critWriteValue = Optional.of(value);
        this.hasRequestedCritWrite = true;
        this.critWriteVotingCount = 0;

        /*
        1. Send CritWriteRequestMessage to all L1s + timeout
        -- On timeout unlock

        2. L1s lock data as well, also send CritWriteRequestMessage + timeout
        -- on timeout unlock + forward abort to DB

        3. L2s lock data, send CriteWriteVoteMessage to L1

        4. L1 collects from L2s, if all OK -> CriteWriteVoteMessage with OK to DB, OTHERWISE abort

        5. DB collects from L1s, if all OK -> update value + send commit to all L1s, OTHERWISE abort to all L1s

        6. On COMMIT: L1 updates value + unlock + send COMMIT to L2s, On ABORT -> unlock + send abort to all L2s

        7. On COMMIT: L2 updates value + unlock data + writeconfirm to client, On ABORT: unlock + simply timeout
         */

        // TODO Check nochmal AC, wenn alle VOTE_OK, erst dann kann gelockt werden, dannach kann erst gupdatetd werde
        // VLL extra COMMIT_LOCK, COMMIT_UPDATE, COMMIT_UNLOCK ??
        // ODER bei L1 und L2 nach VOTE_OK antwort timeout, wenn timeout dann abort, wenn commit dann update
    }

    private void onCritWriteVoteMessage(CritWriteVoteMessage message) {
        if (!message.isOk()) {
            // abort
            this.abortCritWrite();
            return;
        }

        if (this.hasRequestedCritWrite) {
            // increment count
            this.critWriteVotingCount = this.critWriteVotingCount + 1;
            if (this.critWriteVotingCount == this.l1Caches.size()) {
                int key = message.getKey();
                int value = this.critWriteValue.get();
                // all L1s have answered OK
                this.data.setValueForKey(key, value);
                int updateCount = this.data.getUpdateCountForKey(key).get();
                this.data.lockValueForKey(key);
                // now all participants have locked the data, then send a commit message to update the value
                // todo make own method
                CritWriteCommitMessage commitMessage = new CritWriteCommitMessage(key, value, updateCount);
                this.multicast(commitMessage, this.l1Caches);
            }
        }
    }

    private void onReadMessage(ReadMessage message) {
        int key = message.getKey();
        System.out.printf("Database received read message for key %d\n", key);
        this.readAndResponseFill(key, this.getSender());
    }

    private void onCritReadMessage(CritReadMessage message) {
        int key = message.getKey();
        System.out.printf("Database - Received crit read message for key %d\n", key);
        this.readAndResponseFill(key, this.getSender());
    }

    @Override
    protected void onTimeoutMessage(TimeoutMessage message) {
        if (message.getType() == TimeoutType.CRIT_WRITE_REQUEST && this.hasRequestedCritWrite) {
            this.abortCritWrite();
        }
    }

    @Override
    public Receive createReceive() {
       return this
               .receiveBuilder()
               .match(JoinL1CachesMessage.class, this::onJoinL1Caches)
               .match(JoinL2CachesMessage.class, this::onJoinL2Caches)
               .match(WriteMessage.class, this::onWriteMessage)
               .match(CritWriteMessage.class, this::onCritWriteMessage)
               .match(CritWriteVoteMessage.class, this::onCritWriteVoteMessage)
               .match(ReadMessage.class, this::onReadMessage)
               .match(CritReadMessage.class, this::onCritReadMessage)
               .match(TimeoutMessage.class, this::onTimeoutMessage)
               .build();
    }

}
