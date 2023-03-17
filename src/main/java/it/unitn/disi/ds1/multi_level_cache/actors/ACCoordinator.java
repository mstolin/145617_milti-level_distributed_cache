package it.unitn.disi.ds1.multi_level_cache.actors;

import it.unitn.disi.ds1.multi_level_cache.messages.CritWriteVoteMessage;

import java.util.Optional;

public class ACCoordinator <T extends Coordinator> {

    private final Coordinator coordinator;
    private boolean hasRequestedCritWrite = false;
    private int critWriteVotingCount = 0;
    private Optional<Integer> critWriteKey = Optional.empty();
    private Optional<Integer> critWriteValue = Optional.empty();

    public ACCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public boolean hasRequestedCritWrite() {
        return hasRequestedCritWrite;
    }

    public void setCritWriteConfig(int key, int value) {
        this.critWriteKey = Optional.of(key);
        this.critWriteValue = Optional.of(value);
        this.hasRequestedCritWrite = true;
    }

    public void resetCritWriteConfig(int key) {
        this.hasRequestedCritWrite = false;
        this.critWriteVotingCount = 0;
        this.critWriteKey = Optional.empty();
        this.critWriteValue = Optional.empty();
    }

    public void abortCritWrite(int key) {
        this.resetCritWriteConfig(key);
    }

    public void onCritWriteVoteMessage(CritWriteVoteMessage message) {
        int key = message.getKey();
        if (!message.isOk()) {
            // abort
            this.abortCritWrite(key);
            return;
        }

        if (this.hasRequestedCritWrite) {
            // increment count
            this.critWriteVotingCount = this.critWriteVotingCount + 1;

            if (this.coordinator.haveAllParticipantsVoted(this.critWriteVotingCount)) {
                int value = this.critWriteValue.get();
                this.coordinator.onVoteOk(key, value);
            }
        }
    }

}
