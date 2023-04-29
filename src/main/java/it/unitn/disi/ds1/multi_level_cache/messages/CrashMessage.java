package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;

public class CrashMessage implements Serializable {

    /**
     * Delay of milliseconds
     */
    private final long recoverAfter;

    public CrashMessage(long recoverAfter) {
        this.recoverAfter = recoverAfter;
    }

    public long getRecoverAfter() {
        return recoverAfter;
    }

}
