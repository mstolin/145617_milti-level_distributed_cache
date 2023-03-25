package it.unitn.disi.ds1.multi_level_cache.messages;

import akka.actor.ActorRef;
import it.unitn.disi.ds1.multi_level_cache.messages.utils.CacheCrashConfig;

public class InstantiateReadMessage extends CrashableMessage {

    private final int key;
    private final boolean isCritical;
    private final ActorRef l2Cache;

    public InstantiateReadMessage(int key, ActorRef l2Cache, boolean isCritical,
                                  CacheCrashConfig l1CrashConfig, CacheCrashConfig l2CrashConfig) {
        super(l1CrashConfig, l2CrashConfig);
        this.key = key;
        this.l2Cache = l2Cache;
        this.isCritical = isCritical;
    }

    public int getKey() {
        return key;
    }

    public ActorRef getL2Cache() {
        return l2Cache;
    }

    public boolean isCritical() {
        return isCritical;
    }
    
}
