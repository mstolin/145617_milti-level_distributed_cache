package it.unitn.disi.ds1.multi_level_cache.actors.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class DataEntry {

    private int value;

    /** The updateCount shows how often the value has been written. 1 by default.*/
    private int updateCount = 1;

    public DataEntry(int value) {
        this.value = value;
    }

    public DataEntry(int value, int updateCount) {
        this.value = value;
        this.updateCount = updateCount;
    }

    public int getValue() {
        return value;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public void setUpdateCount(int updateCount) {
        this.updateCount = updateCount;
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

    public void setValueForKey(int key, int value, int updateCount) {
        if (this.containsKey(key)) {
            // update
            this.getData(key).setValue(value);
            this.getData(key).setUpdateCount(updateCount);
        } else {
            // set new
            this.data.put(key, new DataEntry(value, updateCount));
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

    public boolean isNewerOrEqual(int key, int updateCount) {
        if (updateCount <= 0) {
            // if it is 0, then it has never been written
            return false;
        }

        Optional<Integer> currentUpdateCount = this.getUpdateCountForKey(key);
        if (currentUpdateCount.isPresent()) {
            return currentUpdateCount.get() >= updateCount;
        }
        return false;
    }

    public void resetData() {
        this.data = new HashMap<>();
    }

}
