package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;

public class InstantiateWriteMessage extends CrashableMessage {

    private final int key;
    private final int value;
    private final ActorRef l2Cache;
    private final boolean isCritical;

    public InstantiateWriteMessage(int key, int value, ActorRef l2Cache, boolean isCritical,
                                   CacheCrashConfig l1CrashConfig, CacheCrashConfig l2CrashConfig) {
        super(l1CrashConfig, l2CrashConfig);
        this.key = key;
        this.value = value;
        this.l2Cache = l2Cache;
        this.isCritical = isCritical;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    public ActorRef getL2Cache() {
        return l2Cache;
    }

    public boolean isCritical() {
        return isCritical;
    }

}
