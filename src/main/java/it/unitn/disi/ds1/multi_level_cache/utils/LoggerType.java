package it.unitn.disi.ds1.multi_level_cache.utils;

public enum LoggerType {

    WRITE,
    READ,
    CRASH,
    RECOVER,
    REFILL,
    FILL,
    JOIN,
    CRITICAL_WRITE,
    CRITICAL_WRITE_ABORT,
    CRITICAL_WRITE_COMMIT,
    CRITICAL_WRITE_REQUEST,
    CRITICAL_WRITE_VOTE,
    CRITICAL_READ,
    FLUSH,
    ;

    @Override
    public String toString() {
        switch (this) {
            case CRASH -> {
                return "CRASH";
            }
            case CRITICAL_READ -> {
                return "CRITICAL-READ";
            }
            case CRITICAL_WRITE -> {
                return "CRITICAL-WRITE";
            }
            case CRITICAL_WRITE_ABORT -> {
                return "CRITICAL-WRITE-ABORT";
            }
            case CRITICAL_WRITE_COMMIT -> {
                return "CRITICAL-WRITE-COMMIT";
            }
            case CRITICAL_WRITE_REQUEST -> {
                return "CRITICAL-WRITE-REQUEST";
            }
            case CRITICAL_WRITE_VOTE -> {
                return "CRITICAL-WRITE-VOTE";
            }
            case FILL -> {
                return "FILL";
            }
            case FLUSH -> {
                return "FLUSH";
            }
            case JOIN -> {
                return "JOIN";
            }
            case READ -> {
                return "READ";
            }
            case RECOVER -> {
                return "RECOVER";
            }
            case REFILL -> {
                return "REFILL";
            }
            case WRITE -> {
                return "WRITE";
            }
            default -> {
                return "";
            }
        }
    }
}
