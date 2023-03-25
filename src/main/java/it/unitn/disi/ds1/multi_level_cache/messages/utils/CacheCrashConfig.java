package it.unitn.disi.ds1.multi_level_cache.messages.utils;

public class CacheCrashConfig {

    private final boolean crashOnReceive;
    private final boolean crashOnProcessed;
    private final long crashAfterOnReceive;
    private final long recoverAfterOnReceive;
    private final long crashAfterOnProcessed;
    private final long recoverAfterOnProcessed;

    public CacheCrashConfig(boolean crashOnReceive, boolean crashOnProcessed, long crashAfterOnReceive,
                            long recoverAfterOnReceive, long crashAfterOnProcessed, long recoverAfterOnProcessed) {
        this.crashOnReceive = crashOnReceive;
        this.crashOnProcessed = crashOnProcessed;
        this.crashAfterOnReceive = crashAfterOnReceive;
        this.recoverAfterOnReceive = recoverAfterOnReceive;
        this.crashAfterOnProcessed = crashAfterOnProcessed;
        this.recoverAfterOnProcessed = recoverAfterOnProcessed;
    }

    public static CacheCrashConfig empty() {
        return new CacheCrashConfig(false, false, 0, 0, 0, 0);
    }

    public static CacheCrashConfig crashOnReceive(long crashAfter, long recoverAfter) {
        return new CacheCrashConfig(true, false, crashAfter, recoverAfter, 0, 0);
    }

    public static CacheCrashConfig crashOnProcessed(long crashAfter, long recoverAfter) {
        return new CacheCrashConfig(false, true, 0, 0, crashAfter, recoverAfter);
    }

    public boolean isCrashOnReceive() {
        return crashOnReceive;
    }

    public boolean isCrashOnProcessed() {
        return crashOnProcessed;
    }

    public long getCrashAfterOnReceive() {
        return crashAfterOnReceive;
    }

    public long getRecoverAfterOnReceive() {
        return recoverAfterOnReceive;
    }

    public long getCrashAfterOnProcessed() {
        return crashAfterOnProcessed;
    }

    public long getRecoverAfterOnProcessed() {
        return recoverAfterOnProcessed;
    }
}
