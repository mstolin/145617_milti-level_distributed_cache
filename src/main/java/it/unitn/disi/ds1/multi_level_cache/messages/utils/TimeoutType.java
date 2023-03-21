package it.unitn.disi.ds1.multi_level_cache.messages.utils;

public enum TimeoutType {
    WRITE,
    WRITE_CONFIRM,
    READ,
    READ_REPLY,
    RE_FILL,
    FILL,
    CRIT_READ,
    CRIT_WRITE,
    CRIT_WRITE_REQUEST,
    CRIT_WRITE_ABORT,
}
