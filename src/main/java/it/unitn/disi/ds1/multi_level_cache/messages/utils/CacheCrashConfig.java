package it.unitn.disi.ds1.multi_level_cache.messages.utils;

public class CacheCrashConfig {

    private final boolean mustCrash;
    private final long crashDelayMillis;
    private final long recoverDelayMillis;

    public CacheCrashConfig(boolean mustCrash, long crashDelayMillis, long recoverDelayMillis) {
        this.mustCrash = mustCrash;
        this.crashDelayMillis = crashDelayMillis;
        this.recoverDelayMillis = recoverDelayMillis;
    }

    public static CacheCrashConfig empty() {
        return new CacheCrashConfig(false, 0, 0);
    }

    public static CacheCrashConfig create(long crashDelayMillis, long recoverDelayMillis) {
        return new CacheCrashConfig(true, crashDelayMillis, recoverDelayMillis);
    }

    public boolean mustCrash() {
        return this.mustCrash;
    }

    public long getCrashDelayMillis() {
        return this.crashDelayMillis;
    }

    public long getRecoverDelayMillis() {
        return this.recoverDelayMillis;
    }

}
