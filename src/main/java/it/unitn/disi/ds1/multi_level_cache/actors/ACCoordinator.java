package it.unitn.disi.ds1.multi_level_cache.actors;

import it.unitn.disi.ds1.multi_level_cache.messages.CritWriteVoteMessage;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.Logger;
import it.unitn.disi.ds1.multi_level_cache.utils.Logger.LoggerOperationType;

import java.util.Optional;

public class ACCoordinator <T extends Coordinator> {

    private final Coordinator coordinator;
    private boolean hasRequestedCritWrite = false;
    private int critWriteVotingCount = 0;
    private Optional<Integer> critWriteValue = Optional.empty();

    public ACCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public boolean hasRequestedCritWrite() {
        return hasRequestedCritWrite;
    }

    public void setCritWriteConfig(int value) {
        this.critWriteValue = Optional.of(value);
        this.hasRequestedCritWrite = true;
    }

    public void resetCritWriteConfig() {
        this.hasRequestedCritWrite = false;
        this.critWriteVotingCount = 0;
        this.critWriteValue = Optional.empty();
    }

    public void onCritWriteVoteMessage(CritWriteVoteMessage message) {
        int key = message.getKey();
        if (!message.isOk()) {
            // abort
            this.coordinator.abortCritWrite(key);
            return;
        }

        if (this.hasRequestedCritWrite) {
            // increment count
            this.critWriteVotingCount = this.critWriteVotingCount + 1;
            int value = this.critWriteValue.get();
            boolean haveAllVoted = this.coordinator.haveAllParticipantsVoted(this.critWriteVotingCount);

            if (haveAllVoted) {
                this.coordinator.onVoteOk(key, value);
            }
        }
    }

}
