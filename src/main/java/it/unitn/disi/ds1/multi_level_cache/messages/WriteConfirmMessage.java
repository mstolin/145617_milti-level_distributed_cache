package it.unitn.disi.ds1.multi_level_cache.messages;

import java.io.Serializable;

public class WriteConfirmMessage implements Serializable {

    private final WriteMessage writeMessage;

    public WriteConfirmMessage(WriteMessage writeMessage) {
        this.writeMessage = writeMessage;
    }

    public WriteMessage getWriteMessage() {
        return writeMessage;
    }
}
