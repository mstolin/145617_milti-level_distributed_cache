package it.unitn.disi.ds1.multi_level_cache.actors;

import akka.actor.AbstractActor;
import it.unitn.disi.ds1.multi_level_cache.actors.utils.DataStore;

import java.util.Optional;

public abstract class DataNode extends AbstractActor {

    private DataStore data = new DataStore();

    protected void lockKey(int key) {
        this.data.lockValueForKey(key);
    }

    protected void unlockKey(int key) {
        this.data.unLockValueForKey(key);
    }

    protected Optional<Integer> getValue(int key) {
        return this.data.getValueForKey(key);
    }

    protected int getValueOrElse(int key) {
        return this.data.getValueForKey(key).orElse(-1);
    }

    protected Optional<Integer> getUpdateCount(int key) {
        return this.data.getUpdateCountForKey(key);
    }

    protected int getUpdateCountOrElse(int key) {
        return this.data.getUpdateCountForKey(key).orElse(0);
    }

    protected void setValue(int key, int value) throws IllegalAccessException {
        this.data.setValueForKey(key, value);
    }

    protected void setValue(int key, int value, int updateCount) throws IllegalAccessException {
        this.data.setValueForKey(key, value, updateCount);
    }

    protected boolean isKeyLocked(int key) {
        return this.data.isLocked(key);
    }

    protected boolean isKeyAvailable(int key) {
        return this.data.containsKey(key);
    }

    protected void flushData() {
        this.data = new DataStore();
    }

}
