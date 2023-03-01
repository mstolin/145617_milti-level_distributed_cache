package it.unitn.disi.ds1.multi_level_cache.actors.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class DataEntry {

    private int value;

    private int updateCount = 0;

    public DataEntry(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public void updateValue(int value) {
        this.value = value;
        this.updateCount = this.updateCount + 1;
    }

}

public class DataStore {

    private Map<Integer, DataEntry> data = new HashMap<>();

    private DataEntry getData(int key) {
        return this.data.get(key);
    }

    public boolean containsKey(int key) {
        return this.data.containsKey(key);
    }

    public void setValueForKey(int key, int value) {
        if (this.containsKey(key)) {
            // update
            this.getData(key).updateValue(value);
        } else {
            // set new
            this.data.put(key, new DataEntry(value));
        }
    }

    public Optional<Integer> getValueForKey(int key) {
        if (this.containsKey(key)) {
            int value = this.getData(key).getValue();
            return Optional.of(value);
        }
        return Optional.empty();
    }

    public Optional<Integer> getUpdateCountForKey(int key) {
        if (this.containsKey(key)) {
            int updateCount = this.getData(key).getUpdateCount();
            return Optional.of(updateCount);
        }
        return Optional.empty();
    }

}
