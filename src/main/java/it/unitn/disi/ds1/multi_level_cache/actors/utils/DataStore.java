package it.unitn.disi.ds1.multi_level_cache.actors.utils;

import java.util.*;

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
    private List<Integer> lockedKeys = new ArrayList<>();

    private DataEntry getData(int key) {
        return this.data.get(key);
    }

    public boolean containsKey(int key) {
        return this.data.containsKey(key);
    }

    public void setValueForKey(int key, int value) throws IllegalAccessException {
        if (this.isLocked(key)) {
            throw new IllegalAccessException();
        }

        if (this.containsKey(key)) {
            // update
            this.getData(key).updateValue(value);
        } else {
            // set new
            this.data.put(key, new DataEntry(value));
        }
    }

    public void setValueForKey(int key, int value, int updateCount) throws IllegalAccessException {
        if (this.isLocked(key)) {
            throw new IllegalAccessException();
        }

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

    public void lockValueForKey(int key) {
        if (!this.lockedKeys.contains(key)) {
            this.lockedKeys.add(key);
        }
    }

    public void unLockValueForKey(int key) {
        if (this.isLocked(key)) {
            int index = this.lockedKeys.indexOf(key);
            this.lockedKeys.remove(index);
        }
    }

    public void unLockAll() {
        this.lockedKeys = new ArrayList<>();
    }

    public boolean isLocked(int key) {
        return this.lockedKeys.contains(key);
    }

    public void resetData() {
        this.data = new HashMap<>();
        this.unLockAll();
    }

}
