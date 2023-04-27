package it.unitn.disi.ds1.multi_level_cache.utils.Logger;

import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageType;

public final class Logger {

    private final static long START_TIME = System.currentTimeMillis();

    private final static String CRASH_FORMAT = "recover-after: %ds";
    private final static String CRITICAL_READ_FORMAT_REC = "key: %d, msg-uc: %d, actor-uc: %d, is-locked: %b";
    private final static String CRITICAL_READ_FORMAT_SEND = "key: %d, uc: %d";
    private final static String CRITICAL_WRITE_ABORT_FORMAT = "key: %d";
    private final static String CRITICAL_WRITE_REQUEST_REC = "key: %d";
    private final static String CRITICAL_WRITE_REQUEST_SEND = "key: %d";
    private final static String CRITICAL_WRITE_VOTE = "key: %d, is-ok: %b";
    private final static String ERROR_FORMAT = "key: %d, msg-type: %s, force-timeout: %b, description: %s";
    private final static String FILL_FORMAT_REC = "key: %d, new-value: %d, old-value: %d, new-uc: %d, old-uc: %d";
    private final static String FILL_FORMAT_SEND = "key: %d, value: %d, uc: %d";
    private final static String INIT_READ_FORMAT = "key: %d, is-critical: %b";
    private final static String INIT_WRITE_FORMAT = "key: %d, value: %d, is-critical: %b";
    private final static String JOIN_FORMAT = "%s of %d";
    private final static String LOG_FORMAT = "%-9.9s | %-8.8s | %-3.3s | %-18.18s | %s";
    private final static String READ_FORMAT_REC = "key: %d, msg-uc: %d, actor-uc: %d, is-locked: %b, is-older: %b, is-unconfirmed: %b";
    private final static String READ_FORMAT_SEND = "key: %d, uc: %d";
    private final static String REFILL_FORMAT = "key: %d, new-value: %d, old-value: %d, msg-uc: %d, actor-uc: %d, is-locked: %b, is-unconfirmed: %b, must-update: %b";
    private final static String WRITE_FORMAT_REC = "key: %d, value: %d, is-locked: %b";
    private final static String WRITE_FORMAT_SEND = "key: %d, value: %d";
    private final static String TIMEOUT_FORMAT = "type: %s";

    private static boolean isSendAction(LoggerOperationType operationType) {
        return operationType == LoggerOperationType.SEND || operationType == LoggerOperationType.MULTICAST || operationType == LoggerOperationType.RETRY;
    }

    public static void printHeader() {
        String header = String.format(LOG_FORMAT, "TIME", "ID", "ACT", "MESSAGE TYPE", "INFO");
        String line = String.format(LOG_FORMAT, "-".repeat(9), "-".repeat(8), "-".repeat(3), "-".repeat(18), "-".repeat(30));
        System.out.println(header);
        System.out.println(line);
    }

    public static void log(MessageType type, String id, LoggerOperationType operationType, String info) {
        long timePassed = System.currentTimeMillis() - START_TIME;
        long minutes = timePassed / 60000;
        long seconds = timePassed / 1000;
        long millis = timePassed % 1000;
        String time = String.format("%02d:%02d,%03d", minutes, seconds, millis);

        info = info == null ? "" : info;
        String msg = String.format(LOG_FORMAT, time, id, operationType, type, info);
        System.out.println(msg);
    }

    public static void crash(String id, long recoverAfter) {
        String msg = String.format(CRASH_FORMAT, recoverAfter);
        log(MessageType.CRASH, id, LoggerOperationType.RECEIVED, msg);
    }

    public static void criticalRead(String id, LoggerOperationType operationType, int key, int msgUpdateCount, int actorUpdateCount, boolean isLocked) {
        String msg = "";

        if (operationType == LoggerOperationType.RECEIVED) {
            msg = String.format(CRITICAL_READ_FORMAT_REC, key, msgUpdateCount, actorUpdateCount, isLocked);
        } else if (isSendAction(operationType)) {
            msg = String.format(CRITICAL_READ_FORMAT_SEND, key, msgUpdateCount);
        }

        log(MessageType.CRITICAL_READ, id, operationType, msg);
    }

    public static void criticalWrite(String id, LoggerOperationType operationType, int key, int value, boolean isLocked) {
        String msg = "";

        if (operationType == LoggerOperationType.RECEIVED) {
            msg = String.format(WRITE_FORMAT_REC, key, value, isLocked);
        } else if (isSendAction(operationType)) {
            msg = String.format(WRITE_FORMAT_SEND, key, value);
        }

        log(MessageType.CRITICAL_WRITE, id, operationType, msg);
    }

    public static void criticalWriteAbort(String id, LoggerOperationType operationType, int key) {
        String msg = String.format(CRITICAL_WRITE_ABORT_FORMAT, key);
        log(MessageType.CRITICAL_WRITE_ABORT, id, operationType, msg);
    }

    public static void criticalWriteCommit(String id, LoggerOperationType operationType, int key, int newValue, int oldValue, int newUc, int oldUc) {
        String msg = "";

        if (operationType == LoggerOperationType.RECEIVED) {
            msg = String.format(FILL_FORMAT_REC, key, newValue, oldValue, newUc, oldUc);
        } else if (isSendAction(operationType)) {
            msg = String.format(FILL_FORMAT_SEND, key, newValue, newUc);
        }

        log(MessageType.CRITICAL_WRITE_COMMIT, id, operationType, msg);
    }

    public static void criticalWriteRequest(String id, LoggerOperationType operationType, int key, boolean isOk) {
        String msg = "";

        if (operationType == LoggerOperationType.RECEIVED) {
            msg = String.format(CRITICAL_WRITE_REQUEST_REC, key, isOk);
        } else if (isSendAction(operationType)) {
            msg = String.format(CRITICAL_WRITE_REQUEST_SEND, key);
        }

        log(MessageType.CRITICAL_WRITE_REQUEST, id, operationType, msg);
    }

    public static void criticalWriteVote(String id, LoggerOperationType operationType, int key, boolean isOk) {
        String msg = String.format(CRITICAL_WRITE_VOTE, key, isOk);
        log(MessageType.CRITICAL_WRITE_VOTE, id, operationType, msg);
    }

    public static void error(String id, LoggerOperationType operationType, MessageType messageType, int key, boolean forceTimeout, String description) {
        String msg = String.format(ERROR_FORMAT, key, messageType, forceTimeout, description);
        log(MessageType.ERROR, id, operationType, msg);
    }

    public static void fill(String id, LoggerOperationType operationType, int key, int newValue, int oldValue, int newUc, int oldUc) {
        String msg = "";

        if (operationType == LoggerOperationType.RECEIVED) {
            msg = String.format(FILL_FORMAT_REC, key, newValue, oldValue, newUc, oldUc);
        } else if (isSendAction(operationType)) {
            msg = String.format(FILL_FORMAT_SEND, key, newValue, newUc);
        }

        log(MessageType.FILL, id, operationType, msg);
    }

    public static void flush(String id, LoggerOperationType operationType) {
        log(MessageType.FLUSH, id, operationType, null);
    }

    public static void initRead(String id, int key, boolean isCritical) {
        String msg = String.format(INIT_READ_FORMAT, key, isCritical);
        log(MessageType.INIT_READ, id, LoggerOperationType.RECEIVED, msg);
    }

    public static void initWrite(String id, int key, int value, boolean isCritical) {
        String msg = String.format(INIT_WRITE_FORMAT, key, value, isCritical);
        log(MessageType.INIT_WRITE, id, LoggerOperationType.RECEIVED, msg);
    }

    public static void join(String id, String groupName, int groupSize) {
        String msg = String.format(JOIN_FORMAT, groupName, groupSize);
        log(MessageType.JOIN, id, LoggerOperationType.RECEIVED, msg);
    }

    public static void read(String id, LoggerOperationType operationType, int key, int msgUpdateCount, int actorUpdateCount, boolean isLocked, boolean isOlder, boolean isUnconfirmed) {
        String msg = "";

        if (operationType == LoggerOperationType.RECEIVED) {
            msg = String.format(READ_FORMAT_REC, key, msgUpdateCount, actorUpdateCount, isLocked, isOlder, isUnconfirmed);
        } else if (isSendAction(operationType)) {
            msg = String.format(READ_FORMAT_SEND, key, msgUpdateCount);
        }

        log(MessageType.READ, id, operationType, msg);
    }

    public static void readReply(String id, LoggerOperationType operationType, int key, int newValue, int oldValue, int newUc, int oldUc) {
        String msg = "";

        if (operationType == LoggerOperationType.RECEIVED) {
            msg = String.format(FILL_FORMAT_REC, key, newValue, oldValue, newUc, oldUc);
        } else if (isSendAction(operationType)) {
            msg = String.format(FILL_FORMAT_SEND, key, newValue, newUc);
        }

        log(MessageType.READ_REPLY, id, operationType, msg);
    }

    public static void recover(String id, LoggerOperationType operationType) {
        log(MessageType.RECOVER, id, operationType, null);
    }

    public static void refill(String id, LoggerOperationType operationType, int key, int newValue, int oldValue, int msgUc, int actorUc, boolean isLocked, boolean isUnconfirmed, boolean mustUpdate) {
        String msg = "";

        if (operationType == LoggerOperationType.RECEIVED) {
            msg = String.format(REFILL_FORMAT, key, newValue, oldValue, msgUc, actorUc, isLocked, isUnconfirmed, mustUpdate);
        } else if (isSendAction(operationType)) {
            msg = String.format(FILL_FORMAT_SEND, key, newValue, msgUc);
        }

        log(MessageType.REFILL, id, operationType, msg);
    }

    public static void timeout(String id, MessageType type) {
        String msg = String.format(TIMEOUT_FORMAT, type);
        log(MessageType.TIMEOUT, id, LoggerOperationType.RECEIVED, msg);
    }

    public static void write(String id, LoggerOperationType operationType, int key, int value, boolean isLocked) {
        String msg = "";

        if (operationType == LoggerOperationType.RECEIVED) {
            msg = String.format(WRITE_FORMAT_REC, key, value, isLocked);
        } else if (isSendAction(operationType)) {
            msg = String.format(WRITE_FORMAT_REC, key, value, isLocked);
        }

        log(MessageType.WRITE, id, operationType, msg);
    }

    public static void writeConfirm(String id, LoggerOperationType operationType, int key, int newValue, int oldValue, int newUc, int oldUc) {
        String msg = "";

        if (operationType == LoggerOperationType.RECEIVED) {
            msg = String.format(FILL_FORMAT_REC, key, newValue, oldValue, newUc, oldUc);
        } else if (isSendAction(operationType)) {
            msg = String.format(FILL_FORMAT_SEND, key, newValue, newUc);
        }

        log(MessageType.WRITE_CONFIRM, id, operationType, msg);
    }

}
