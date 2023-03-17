package it.unitn.disi.ds1.multi_level_cache.actors;

import it.unitn.disi.ds1.multi_level_cache.messages.CritWriteVoteMessage;

import java.util.Optional;

public abstract class ACCoordinator extends Node {

    protected boolean hasRequestedCritWrite = false;
    protected int critWriteVotingCount = 0;
    protected Optional<Integer> critWriteKey = Optional.empty();
    protected Optional<Integer> critWriteValue = Optional.empty();

    public ACCoordinator(String id) {
        super(id);
    }

    /**
     * Determines if all participants of the round have voted.
     *
     * @return Boolean stating that all participants have voted or not
     */
    protected abstract boolean haveAllParticipantsVoted();

    /**
     * Is getting called when all participants voted ok.
     */
    protected abstract void onVoteOk(int key);

    protected void resetCritWriteConfig(int key) {
        this.hasRequestedCritWrite = false;
        this.critWriteVotingCount = 0;
        this.critWriteKey = Optional.empty();
        this.critWriteValue = Optional.empty();
        this.data.unLockValueForKey(key);
    }

    protected void abortCritWrite(int key) {
        this.resetCritWriteConfig(key);
    }

    protected void onCritWriteVoteMessage(CritWriteVoteMessage message) {
        int key = message.getKey();
        if (!message.isOk()) {
            // abort
            this.abortCritWrite(key);
            return;
        }

        if (this.hasRequestedCritWrite) {
            // increment count
            this.critWriteVotingCount = this.critWriteVotingCount + 1;

            if (this.haveAllParticipantsVoted()) {
                this.onVoteOk(key);
            }
        }
    }

}
