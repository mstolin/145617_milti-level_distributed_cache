package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;

import java.util.*;

public class Database extends Node {

    private List<ActorRef> l1Caches;
    private List<ActorRef> l2Caches;
    private Map<Integer, List<ActorRef>> unconfirmedReads = new HashMap<>();
    private boolean hasRequestedCritWrite = false;
    private int critWriteVotingCount = 0;
    private Optional<Integer> critWriteKey = Optional.empty();
    private Optional<Integer> critWriteValue = Optional.empty();

    public Database() {
        super("Database");

        try {
            this.setDefaultData(10);
        } catch (IllegalAccessException e) {
            System.out.printf("%s - Wasn't able to set default data\n", this.id);
        }

    }

    static public Props props() {
        return Props.create(Database.class, () -> new Database());
    }

    private void setDefaultData(int size) throws IllegalAccessException {
        for (int i = 0; i < size; i++) {
            int value = new Random().nextInt(1000);
            this.data.setValueForKey(i, value, 1);
        }
    }

    private void responseFill(int key) {
        Optional<Integer> value = this.data.getValueForKey(key);
        Optional<Integer> updateCount = this.data.getUpdateCountForKey(key);
        if (this.isReadUnconfirmed(key) && value.isPresent() && updateCount.isPresent()) {
            System.out.printf("Database - Requested value is %d, send fill message to sender\n", value.get());
            // multicast to everyone who has requested the value
            List<ActorRef> senders = this.unconfirmedReads.get(key);
            FillMessage fillMessage = new FillMessage(key, value.get(), updateCount.get());
            this.multicast(fillMessage, senders);
            // reset the config
            this.resetReadConfig(key);
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

        try {
            // write data
            this.data.setValueForKey(key, value);

            // Lock data until write confirm and refill has been sent
            this.data.lockValueForKey(key);

            // we can be sure it exists, since we set value previously
            int updateCount = this.data.getUpdateCountForKey(key).get();

            // Send confirm to L1 sender
            // todo make own method
            WriteConfirmMessage writeConfirmMessage = new WriteConfirmMessage(
                    message.getKey(), message.getValue(), updateCount);
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
        } catch(IllegalAccessException e) {
            // force timeout, either locked by another write or critical write
        }
    }

    private void onCritWriteMessage(CritWriteMessage message) {
        int key = message.getKey();
        int value = message.getValue();;
        System.out.printf("Database - Received critical write message of {%d: %d}\n", key, value);

        if (this.data.isLocked(key)) {
            System.out.printf("Database - Can't write key %d is locked\n", key);
            return; // todo send error
        }

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
            // check if all L1s have answered
            if (this.critWriteVotingCount == this.l1Caches.size()) {
                int key = message.getKey();
                int value = this.critWriteValue.get();

                // all L1s have answered OK
                try {
                    this.data.setValueForKey(key, value);

                    int updateCount = this.data.getUpdateCountForKey(key).get();
                    this.data.lockValueForKey(key);
                    // now all participants have locked the data, then send a commit message to update the value
                    // todo make own method
                    CritWriteCommitMessage commitMessage = new CritWriteCommitMessage(key, value, updateCount);
                    this.multicast(commitMessage, this.l1Caches);
                } catch (IllegalAccessException e) {
                    // already locked -> force timeout
                }
            }
        }
    }

    private void onReadMessage(ReadMessage message) {
        int key = message.getKey();
        System.out.printf("Database - Received read message for key %d\n", key);

        // add read as unconfirmed
        this.addUnconfirmedReadMessage(key, this.getSender());
        // lock value until fill has been sent
        this.data.lockValueForKey(key);
        // send fill message
        this.responseFill(key);
    }

    private void onCritReadMessage(CritReadMessage message) {
        int key = message.getKey();
        System.out.printf("Database - Received crit read message for key %d\n", key);

        // add read as unconfirmed
        this.addUnconfirmedReadMessage(key, this.getSender());
        // lock value until fill has been sent
        this.data.lockValueForKey(key);
        // send fill message
        this.responseFill(key);
    }

    @Override
    protected boolean canInstantiateNewWriteConversation(int key) {
        return !this.data.isLocked(key);
    }

    @Override
    protected boolean canInstantiateNewReadConversation(int key) {
        return !this.data.isLocked(key);
    }

    @Override
    protected void onTimeoutMessage(TimeoutMessage message) {
        if (message.getType() == TimeoutType.CRIT_WRITE_REQUEST && this.hasRequestedCritWrite) {
            this.abortCritWrite();
        }
    }

    @Override
    protected void addUnconfirmedReadMessage(int key, ActorRef sender) {
        if (this.unconfirmedReads.containsKey(key)) {
            this.unconfirmedReads.get(key).add(sender);
        } else {
            List<ActorRef> senders = new ArrayList<>(List.of(sender));
            this.unconfirmedReads.put(key, senders);
        }
    }

    @Override
    protected boolean isReadUnconfirmed(int key) {
        return this.unconfirmedReads.containsKey(key);
    }

    @Override
    protected void resetReadConfig(int key) {
        if (this.unconfirmedReads.containsKey(key)) {
            this.unconfirmedReads.remove(key);
        }
        this.data.unLockValueForKey(key);
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
