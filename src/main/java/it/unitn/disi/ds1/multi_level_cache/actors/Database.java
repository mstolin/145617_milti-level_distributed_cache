package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.TimeoutType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerType;

import java.util.*;

public class Database extends Node implements Coordinator {

    private final ACCoordinator acCoordinator = new ACCoordinator(this);
    private List<ActorRef> l1Caches;
    private List<ActorRef> l2Caches;
    private Map<Integer, List<ActorRef>> unconfirmedReads = new HashMap<>();

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
            // multicast to everyone who has requested the value
            List<ActorRef> senders = this.unconfirmedReads.get(key);
            FillMessage fillMessage = new FillMessage(key, value.get(), updateCount.get());
            Logger.fill(this.id, LoggerOperationType.MULTICAST, key, value.get(), 0, updateCount.get(), 0);
            this.multicast(fillMessage, senders);
            // reset the config
            this.resetReadConfig(key);
        } else {
            Logger.error(this.id, LoggerType.READ, key, true, "Key is unknown");
            // todo send error response
        }
    }

    private void onJoinL1Caches(JoinL1CachesMessage message) {
        this.l1Caches = List.copyOf(message.getL1Caches());
        Logger.join(this.id, "L1 Caches", this.l1Caches.size());
    }

    private void onJoinL2Caches(JoinL2CachesMessage message) {
        this.l2Caches = List.copyOf(message.getL2Caches());
        Logger.join(this.id, "L2 Caches", this.l2Caches.size());
    }

    private void onWriteMessage(WriteMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        boolean isLocked = this.data.isLocked(key);

        if (!this.canInstantiateNewWriteConversation(key)) {
            Logger.error(this.id, LoggerType.WRITE, key, true,
                    "Can't write value, because key is locked or unconfirmed");
            return;
        }

        Logger.write(this.id, LoggerOperationType.RECEIVED, key, value, isLocked);

        try {
            // write data
            this.data.setValueForKey(key, value);

            // Lock data until write confirm and refill has been sent
            this.data.lockValueForKey(key);

            // we can be sure it exists, since we set value previously
            int updateCount = this.data.getUpdateCountForKey(key).get();

            // Send refill to all other L1 caches
            // todo make own method
            RefillMessage refillMessage = new RefillMessage(key, value, updateCount);
            Logger.refill(this.id, LoggerOperationType.MULTICAST, key, value, 0, updateCount, 0, false, false, false);
            this.multicast(refillMessage, this.l1Caches);

            // Unlock value
            this.data.unLockValueForKey(key);
        } catch(IllegalAccessException e) {
            // force timeout, either locked by another write or critical write
        }
    }

    private void onCritWriteMessage(CritWriteMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        boolean isLocked = this.data.isLocked(key);

        if (!this.canInstantiateNewWriteConversation(key)) {
            Logger.error(this.id, LoggerType.CRITICAL_WRITE, key, true,
                    "Can't write value, because key is locked or unconfirmed");
            return;
        }

        Logger.criticalWrite(this.id, LoggerOperationType.RECEIVED, key, value, isLocked);

        // Multicast vote request to all L1s // todo make own method
        CritWriteRequestMessage critWriteRequestMessage = new CritWriteRequestMessage(key);
        Logger.criticalWriteRequest(this.id, LoggerOperationType.MULTICAST, key, true);
        this.multicast(critWriteRequestMessage, this.l1Caches);
        this.setMulticastTimeout(critWriteRequestMessage, TimeoutType.CRIT_WRITE_REQUEST);
        // set crit write config
        this.acCoordinator.setCritWriteConfig(value);

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
        this.acCoordinator.onCritWriteVoteMessage(message);
    }

    private void onReadMessage(ReadMessage message) {
        int key = message.getKey();

        if (!this.canInstantiateNewReadConversation(key)) {
            // force timeout
            Logger.error(this.id, LoggerType.READ, key, true, "Can't read value, because it's locked");
            return;
        }
        Logger.read(this.id, LoggerOperationType.RECEIVED, key, message.getUpdateCount(), this.data.getUpdateCountForKey(key).orElse(0),
                this.data.isLocked(key), false);

        // add read as unconfirmed
        this.addUnconfirmedReadMessage(key, this.getSender());
        // lock value until fill has been sent
        this.data.lockValueForKey(key);
        // send fill message
        this.responseFill(key);
    }

    private void onCritReadMessage(CritReadMessage message) {
        int key = message.getKey();
        if (!this.canInstantiateNewReadConversation(key)) {
            // force timeout
            Logger.error(this.id, LoggerType.CRITICAL_READ, key, true, "Can't read value, because it's locked");
            return;
        }

        Logger.criticalRead(this.id, LoggerOperationType.RECEIVED, key, message.getUpdateCount(),
                this.data.getUpdateCountForKey(key).orElse(0), this.data.isLocked(key));

        // add read as unconfirmed
        this.addUnconfirmedReadMessage(key, this.getSender());
        // lock value until fill has been sent
        this.data.lockValueForKey(key);
        // send fill message
        this.responseFill(key);
    }

    @Override
    public boolean haveAllParticipantsVoted(int voteCount) {
        return voteCount == this.l1Caches.size();
    }

    @Override
    public void onVoteOk(int key, int value) {
        // update value
        try {
            this.data.setValueForKey(key, value);

            int updateCount = this.data.getUpdateCountForKey(key).get();
            this.data.lockValueForKey(key);
            // now all participants have locked the data, then send a commit message to update the value
            // todo make own method
            CritWriteCommitMessage commitMessage = new CritWriteCommitMessage(key, value, updateCount);
            Logger.criticalWriteCommit(this.id, LoggerOperationType.MULTICAST, key, value, 0, updateCount, 0);
            this.multicast(commitMessage, this.l1Caches);
        } catch (IllegalAccessException e) {
            // already locked -> force timeout
        }
    }

    @Override
    public void abortCritWrite(int key) {
        this.acCoordinator.resetCritWriteConfig();
        this.data.unLockValueForKey(key);
        // multicast abort message
        CritWriteAbortMessage abortMessage = new CritWriteAbortMessage(key);
        Logger.criticalWriteAbort(this.id, LoggerOperationType.MULTICAST, key);
        this.multicast(abortMessage, this.l1Caches);
    }

    @Override
    protected boolean canInstantiateNewWriteConversation(int key) {
        return !this.data.isLocked(key); // todo isWriteUnconfirmed
    }

    @Override
    protected boolean canInstantiateNewReadConversation(int key) {
        return !this.data.isLocked(key);
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

    private void onTimeoutMessage(TimeoutMessage message) {
        if (message.getType() == TimeoutType.CRIT_WRITE_REQUEST && this.acCoordinator.hasRequestedCritWrite()) {
            CritWriteRequestMessage requestMessage = (CritWriteRequestMessage) message.getMessage();
            Logger.timeout(this.id, TimeoutType.CRIT_WRITE_ABORT);
            this.abortCritWrite(requestMessage.getKey());
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
