package it.unitn.disi.ds1.multi_level_cache.actors;

import java.util.UUID;

public interface Coordinator {

    /**
     * Determines if all participants of the round have voted.
     *
     * @return Boolean stating that all participants have voted or not
     */
    boolean haveAllParticipantsVoted(int voteCount);

    /**
     * Is getting called when all participants voted ok.
     */
    void onVoteOk(UUID uuid, int key, int value);

    void abortCritWrite(UUID uuid, int key);

}
