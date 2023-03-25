package it.unitn.disi.ds1.multi_level_cache.messages.utils;

public class CacheCrashConfig {

    private final boolean crashOnReceive;
    private final boolean crashOnProcessed;
    private final long crashAfterMilliseconds;


    public CacheCrashConfig(boolean crashOnReceive, boolean crashOnProcessed, long crashAfterMilliseconds) {
        this.crashOnReceive = crashOnReceive;
        this.crashOnProcessed = crashOnProcessed;
        this.crashAfterMilliseconds = crashAfterMilliseconds;
    }

    public static CacheCrashConfig empty() {
        return new CacheCrashConfig(false, false, 0);
    }

    public boolean isCrashOnReceive() {
        return crashOnReceive;
    }

    public boolean isCrashOnProcessed() {
        return crashOnProcessed;
    }

    public long getCrashAfterMilliseconds() {
        return crashAfterMilliseconds;
    }

}
