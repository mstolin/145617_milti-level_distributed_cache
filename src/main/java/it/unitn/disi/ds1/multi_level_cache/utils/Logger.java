package it.unitn.disi.ds1.multi_level_cache.utils;

public final class Logger {

    private final static String CRITICAL_READ_FORMAT = "key: %d, msg-uc: %d, actor-uc: %d";
    private final static String CRITICAL_WRITE_REQUEST = "key: %d, is-ok: %b";
    private final static String CRITICAL_WRITE_VOTE = "key: %d, value: %d, have-all-voted: %b";
    private final static String FILL_FORMAT = "key: %d, new-value: %d, old-value: %d, new-uc: %d, old-uc: %d";
    private final static String JOIN_FORMAT = "%s of %d";
    private final static String READ_FORMAT = "key: %d, msg-uc: %d, actor-uc: %d, forward: %b";
    private final static String REFILL_FORMAT = "key: %d, new-value: %d, old-value: %d, msg-uc: %d, actor-uc: %d, is-locked: %b, must-save: %b";
    private final static String WRITE_FORMAT = "key: %d, value: %d, is-locked: %b";

    public static void log(LoggerType type, String id, String info) {
        info = info == null ? "" : info;
        String msg = String.format("%-10.10s - %-5.5s - %s", String.format("[%s]", id), type, info);
        System.out.println(msg);
    }

    public static void crash(String id) {
        log(LoggerType.CRASH, id, null);
    }

    public static void criticalRead(String id, int key, int msgUpdateCount, int actorUpdateCount) {
        String msg = String.format(CRITICAL_READ_FORMAT, key, msgUpdateCount, actorUpdateCount);
        log(LoggerType.CRITICAL_READ, id, msg);
    }

    public static void criticalWrite(String id, int key, int value, boolean isLocked) {
        String msg = String.format(WRITE_FORMAT, key, value, isLocked);
        log(LoggerType.CRITICAL_WRITE, id, msg);
    }

    public static void criticalWriteAbort(String id) {
        log(LoggerType.CRITICAL_WRITE_REQUEST, id, null);
    }

    public static void criticalWriteCommit(String id, int key, int newValue, int oldValue, int newUc, int oldUc) {
        String msg = String.format(FILL_FORMAT, key, newValue, oldValue, newUc, oldUc);
        log(LoggerType.CRITICAL_WRITE_COMMIT, id, msg);
    }

    public static void criticalWriteRequest(String id, int key, boolean isOk) {
        String msg = String.format(CRITICAL_WRITE_REQUEST, key, isOk);
        log(LoggerType.CRITICAL_WRITE_REQUEST, id, msg);
    }

    public static void criticalWriteVote(String id, int key, int value, boolean haveAllVoted) {
        String msg = String.format(CRITICAL_WRITE_VOTE, key, value, haveAllVoted);
        log(LoggerType.CRITICAL_WRITE_VOTE, id, msg);
    }

    public static void flush(String id) {
        log(LoggerType.FLUSH, id, null);
    }

    public static void fill(String id, int key, int newValue, int oldValue, int newUc, int oldUc) {
        String msg = String.format(FILL_FORMAT, key, newValue, oldValue, newUc, oldUc);
        log(LoggerType.FILL, id, msg);
    }

    public static void join(String id, String groupName, int groupSize) {
        String msg = String.format(JOIN_FORMAT, groupName, groupSize);
        log(LoggerType.JOIN, id, msg);
    }

    public static void read(String id, int key, int msgUpdateCount, int actorUpdateCount, boolean forward) {
        String msg = String.format(READ_FORMAT, key, msgUpdateCount, actorUpdateCount, forward);
        log(LoggerType.READ, id, msg);
    }

    public static void recover(String id) {
        log(LoggerType.RECOVER, id, null);
    }

    public static void refill(String id, int key, int newValue, int oldValue, int msgUc, int actorUc, boolean isLocked, boolean mustSave) {
        String msg = String.format(REFILL_FORMAT, key, newValue, oldValue, msgUc, actorUc, isLocked, mustSave);
        log(LoggerType.REFILL, id, msg);
    }

    public static void write(String id, int key, int value, boolean isLocked) {
        String msg = String.format(WRITE_FORMAT, key, value, isLocked);
        log(LoggerType.WRITE, id, msg);
    }

}
