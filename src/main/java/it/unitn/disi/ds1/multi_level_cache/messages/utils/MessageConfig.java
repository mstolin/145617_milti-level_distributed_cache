package it.unitn.disi.ds1.multi_level_cache.messages.utils;

public class MessageConfig {

    /**
     * The delay for message sending in milliseconds.
     */
    private final long delay;
    private final CacheCrashConfig l1CrashConfig;
    private final CacheCrashConfig l2CrashConfig;

    public MessageConfig(long delay, CacheCrashConfig l1CrashConfig, CacheCrashConfig l2CrashConfig) {
        this.delay = delay;
        this.l1CrashConfig = l1CrashConfig;
        this.l2CrashConfig = l2CrashConfig;
    }

    public static MessageConfig of(long delay, CacheCrashConfig l1CrashConfig, CacheCrashConfig l2CrashConfig) {
        return new MessageConfig(delay, l1CrashConfig, l2CrashConfig);
    }

    public static MessageConfig of(CacheCrashConfig l1CrashConfig, CacheCrashConfig l2CrashConfig) {
        return new MessageConfig(0, l1CrashConfig, l2CrashConfig);
    }

    public static MessageConfig of(long delay) {
        return new MessageConfig(delay, CacheCrashConfig.empty(), CacheCrashConfig.empty());
    }

    public static MessageConfig none() {
        return of(0);
    }

    public long getDelay() {
        return this.delay;
    }

    public CacheCrashConfig getL1CrashConfig() {
        return this.l1CrashConfig;
    }

    public CacheCrashConfig getL2CrashConfig() {
        return this.l2CrashConfig;
    }
}
