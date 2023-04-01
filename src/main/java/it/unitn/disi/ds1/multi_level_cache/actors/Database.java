package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.disi.ds1.multi_level_cache.messages.*;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;

import java.util.*;

public class Database extends Node implements Coordinator {

    private final ACCoordinator acCoordinator = new ACCoordinator(this);
    private List<ActorRef> l1Caches;
    private List<ActorRef> l2Caches;

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

    private ActorRef getActorForUnconfirmedRead(int key) {
        if (this.isReadUnconfirmed(key)) {
            return this.getUnconfirmedActorsForRead(key).get(0);
        }
        return ActorRef.noSender();
    }

    private void setDefaultData(int size) throws IllegalAccessException {
        for (int i = 0; i < size; i++) {
            int value = new Random().nextInt(1000);
            this.setValue(i, value, 1);
        }
    }

    private void responseFill(int key) {
        Optional<Integer> value = this.getValue(key);
        Optional<Integer> updateCount = this.getUpdateCount(key);
        if (this.isReadUnconfirmed(key) && value.isPresent() && updateCount.isPresent()) {
            // multicast to everyone who has requested the value
            ActorRef sender = this.getActorForUnconfirmedRead(key);
            FillMessage fillMessage = new FillMessage(key, value.get(), updateCount.get());
            Logger.fill(this.id, LoggerOperationType.SEND, key, value.get(), 0, updateCount.get(), 0);
            sender.tell(fillMessage, this.getSelf());
            // reset the config
            this.removeUnconfirmedRead(key);
        } else {
            Logger.error(this.id, MessageType.READ, key, true, "Key is unknown");
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
        boolean isLocked = this.isKeyLocked(key);

        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, MessageType.WRITE, key, true,
                    "Can't write value, because key is locked or unconfirmed");
            this.getSender().tell(ErrorMessage.lockedKey(key, MessageType.WRITE), this.getSelf());
            return;
        }

        Logger.write(this.id, LoggerOperationType.RECEIVED, key, value, isLocked);

        try {
            // write data
            this.setValue(key, value);

            // Lock data until write confirm and refill has been sent
            this.lockKey(key);

            // we can be sure it exists, since we set value previously
            int updateCount = this.getUpdateCountOrElse(key);

            // Send refill to all other L1 caches
            // todo make own method
            RefillMessage refillMessage = new RefillMessage(key, value, updateCount);
            Logger.refill(this.id, LoggerOperationType.MULTICAST, key, value, 0, updateCount, 0, false, false, false);
            this.multicast(refillMessage, this.l1Caches);

            // Unlock value
            this.unlockKey(key);
        } catch(IllegalAccessException e) {
            // force timeout, either locked by another write or critical write
        }
    }

    private void onCritWriteMessage(CritWriteMessage message) {
        int key = message.getKey();
        int value = message.getValue();
        boolean isLocked = this.isKeyLocked(key);

        if (this.isKeyLocked(key) || this.isWriteUnconfirmed(key)) {
            Logger.error(this.id, MessageType.CRITICAL_WRITE, key, true,
                    "Can't write value, because key is locked or unconfirmed");
            this.getSender().tell(ErrorMessage.lockedKey(key, MessageType.CRITICAL_WRITE), this.getSelf());
            return;
        }

        Logger.criticalWrite(this.id, LoggerOperationType.RECEIVED, key, value, isLocked);

        // lock value from now on
        this.lockKey(key);
        // Multicast vote request to all L1s // todo make own method
        CritWriteRequestMessage critWriteRequestMessage = new CritWriteRequestMessage(key);
        Logger.criticalWriteRequest(this.id, LoggerOperationType.MULTICAST, key, true);
        this.multicast(critWriteRequestMessage, this.l1Caches);
        this.setMulticastTimeout(critWriteRequestMessage, MessageType.CRITICAL_WRITE_REQUEST);
        // set crit write config
        this.acCoordinator.setCritWriteConfig(value);
    }

    private void onCritWriteVoteMessage(CritWriteVoteMessage message) {
        Logger.criticalWriteVote(this.id, LoggerOperationType.RECEIVED, message.getKey(), message.isOk());
        this.acCoordinator.onCritWriteVoteMessage(message);
    }

    private void onReadMessage(ReadMessage message) {
        int key = message.getKey();

        if (!this.isKeyAvailable(key)) {
            Logger.error(this.id, MessageType.READ, key, false, String.format("Can't read, because key %d is unknown", key));
            this.getSender().tell(ErrorMessage.unknownKey(key, MessageType.READ), this.getSelf());
            return;
        }

        if (this.isKeyLocked(key)) {
            // force timeout
            Logger.error(this.id, MessageType.READ, key, true, "Can't read value, because it's locked");
            this.getSender().tell(ErrorMessage.lockedKey(key, MessageType.READ), this.getSelf());
            return;
        }
        boolean isLocked = this.isKeyLocked(key);
        boolean isUnconfirmed = this.isReadUnconfirmed(key);
        Logger.read(this.id, LoggerOperationType.RECEIVED, key, message.getUpdateCount(), this.getUpdateCountOrElse(key),
                isLocked, false, isUnconfirmed);

        // add read as unconfirmed
        this.addUnconfirmedRead(key, this.getSender());
        // lock value until fill has been sent
        this.lockKey(key);
        // send fill message
        this.responseFill(key);
    }

    private void onCritReadMessage(CritReadMessage message) {
        int key = message.getKey();

        if (!this.isKeyAvailable(key)) {
            Logger.error(this.id, MessageType.CRITICAL_READ, key, false, String.format("Can't read, because key %d is unknown", key));
            this.getSender().tell(ErrorMessage.unknownKey(key, MessageType.CRITICAL_READ), this.getSelf());
            return;
        }

        if (this.isKeyLocked(key)) {
            // force timeout
            Logger.error(this.id, MessageType.CRITICAL_READ, key, true, "Can't read value, because it's locked");
            this.getSender().tell(ErrorMessage.lockedKey(key, MessageType.CRITICAL_READ), this.getSelf());
            return;
        }

        Logger.criticalRead(this.id, LoggerOperationType.RECEIVED, key, message.getUpdateCount(),
                this.getUpdateCountOrElse(key), this.isKeyLocked(key));

        // add read as unconfirmed
        this.addUnconfirmedRead(key, this.getSender());
        // lock value until fill has been sent
        this.lockKey(key); // todo remove
        // send fill message
        this.responseFill(key);
    }

    @Override
    public boolean haveAllParticipantsVoted(int voteCount) {
        return voteCount == this.l1Caches.size();
    }

    @Override
    public void onVoteOk(int key, int value) {
        // reset timeout
        this.acCoordinator.resetCritWriteConfig();

        // update value
        this.unlockKey(key);
        try {
            this.setValue(key, value);

            int updateCount = this.getUpdateCountOrElse(key);
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
        this.unlockKey(key);
        // multicast abort message
        CritWriteAbortMessage abortMessage = new CritWriteAbortMessage(key);
        Logger.criticalWriteAbort(this.id, LoggerOperationType.MULTICAST, key);
        this.multicast(abortMessage, this.l1Caches);
    }


    private void onTimeoutMessage(TimeoutMessage message) {
        if (message.getType() == MessageType.CRITICAL_WRITE_REQUEST && this.acCoordinator.hasRequestedCritWrite()) {
            CritWriteRequestMessage requestMessage = (CritWriteRequestMessage) message.getMessage();
            Logger.timeout(this.id, MessageType.CRITICAL_WRITE_REQUEST);
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
