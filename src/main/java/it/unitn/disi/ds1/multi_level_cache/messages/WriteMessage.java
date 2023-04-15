package it.unitn.disi.ds1.multi_level_cache.messages;

import it.unitn.disi.ds1.multi_level_cache.messages.utils.MessageConfig;

public class WriteMessage extends Message {

    private final int key;
    private final int value;

    public WriteMessage(int key, int value, MessageConfig messageConfig) {
        super(messageConfig);
        this.key = key;
        this.value = value;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

}
