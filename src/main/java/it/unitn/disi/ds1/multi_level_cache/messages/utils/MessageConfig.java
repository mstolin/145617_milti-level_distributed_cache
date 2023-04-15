package it.unitn.disi.ds1.multi_level_cache.messages.utils;

public class MessageConfig {
    private final CacheBehaviourConfig l1BehaviourConfig;
    private final CacheBehaviourConfig l2BehaviourConfig;

    public MessageConfig(CacheBehaviourConfig l1BehaviourConfig, CacheBehaviourConfig l2BehaviourConfig) {
        this.l1BehaviourConfig = l1BehaviourConfig;
        this.l2BehaviourConfig = l2BehaviourConfig;
    }

    public static MessageConfig of(CacheBehaviourConfig l1BehaviourConfig, CacheBehaviourConfig l2BehaviourConfig) {
        return new MessageConfig(l1BehaviourConfig, l2BehaviourConfig);
    }

    public static MessageConfig none() {
        return of(CacheBehaviourConfig.none(), CacheBehaviourConfig.none());
    }

    public boolean isMessageDelayedAtL1() {
        return this.l1BehaviourConfig.getMessageDelay() > 0;
    }

    public boolean isMessageDelayedAtL2() {
        return this.l2BehaviourConfig.getMessageDelay() > 0;
    }

    public long getL1MessageDelay() {
        return this.l1BehaviourConfig.getMessageDelay();
    }

    public long getL2MessageDelay() {
        return this.l2BehaviourConfig.getMessageDelay();
    }

    public boolean mustL1Crash() {
        return this.l1BehaviourConfig.getRecoverDelay() > 0;
    }

    public boolean mustL2Crash() {
        return this.l2BehaviourConfig.getRecoverDelay() > 0;
    }

    public long getL1RecoverDelay() {
        return this.l1BehaviourConfig.getRecoverDelay();
    }

    public long getL2RecoverDelay() {
        return this.l2BehaviourConfig.getRecoverDelay();
    }
}
