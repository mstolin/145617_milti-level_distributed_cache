package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;

public class CrashMessage implements Serializable {

    private final long recoverAfterSeconds;

    public CrashMessage(long recoverAfterSeconds) {
        this.recoverAfterSeconds = recoverAfterSeconds;
    }

    public long getRecoverAfterSeconds() {
        return recoverAfterSeconds;
    }

}
