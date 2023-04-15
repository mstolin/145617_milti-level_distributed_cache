package it.unitn.disi.ds1.multi_level_cache.messages.utils;

public class CacheBehaviourConfig {

    private final long messageDelay;
    private final long recoverDelay;

    public CacheBehaviourConfig(long messageDelay, long recoverDelay) {
        this.messageDelay = messageDelay;
        this.recoverDelay = recoverDelay;
    }

    public static CacheBehaviourConfig delayAndRecover(long messageDelay, long recoverDelay) {
        return new CacheBehaviourConfig(messageDelay, recoverDelay);
    }

    public static CacheBehaviourConfig delayMessage(long messageDelay) {
        return new CacheBehaviourConfig(messageDelay, 0);
    }

    public static CacheBehaviourConfig crashAndRecoverAfter(long recoverDelay) {
        return new CacheBehaviourConfig(0, recoverDelay);
    }

    public static CacheBehaviourConfig none() {
        return new CacheBehaviourConfig(0, 0);
    }

    public long getMessageDelay() {
        return this.messageDelay;
    }

    public long getRecoverDelay() {
        return this.recoverDelay;
    }
}
