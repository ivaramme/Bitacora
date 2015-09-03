package com.grayscaleconsulting.bitacora.storage;

import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;

import com.grayscaleconsulting.bitacora.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.CompressionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of a LocalStorage instance RocksDB
 *
 * Created by ivaramme on 8/20/15.
 */
public class LocalStorageRocksDBImpl implements LocalStorage {
    private Logger logger = LoggerFactory.getLogger(LocalStorageRocksDBImpl.class);
    
    private final Counter setCounter = Metrics.getDefault().newCounter(LocalStorageRocksDBImpl.class, "local-storage-sets");
    private final Counter getCounter = Metrics.getDefault().newCounter(LocalStorageRocksDBImpl.class, "local-storage-gets");
    private final Counter missedCounter = Metrics.getDefault().newCounter(LocalStorageRocksDBImpl.class, "local-storage-missed");
    private final Counter deleteCounter = Metrics.getDefault().newCounter(LocalStorageRocksDBImpl.class, "local-storage-deletes");
    
    private boolean ready = false;
    private RocksDB database;
    private Options options = new Options();
    private String path;

    static {
        RocksDB.loadLibrary();
    }
    
    public LocalStorageRocksDBImpl(String path) {
        this.path = path;
    }
    
    @Override
    public void start() {
        options.setCreateIfMissing(true)
                .createStatistics()
                .setMaxWriteBufferNumber(3)
                .setMaxBackgroundCompactions(10)
                .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                .setCompactionStyle(CompactionStyle.UNIVERSAL);

        try {
            database = RocksDB.open(options, path);
            ready = true;
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void set(String key, KeyValue value) throws Exception {
        checkNotNull(database);
        checkState(ready);
        
        logger.info("Persisting local value for key {}", key);
        
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        ObjectOutputStream outputStream  = new ObjectOutputStream(bao);
        outputStream.writeObject(value);
        database.put(key.getBytes(), bao.toByteArray());
        outputStream.close();
        
        setCounter.inc();
        bao.close();
    }

    @Override
    public KeyValue get(String key) throws Exception {
        checkNotNull(database);
        checkState(ready);

        logger.info("Getting local value for key {}", key);
        
        byte[] bytes = database.get(key.getBytes());
        if(null == bytes) {
            missedCounter.inc();
            return null;
        }
        
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = new ObjectInputStream(bis);
        KeyValue data = (KeyValue) in.readObject();
        getCounter.inc();
        
        return data;
    }

    @Override
    public void delete(String key) throws Exception {
        checkNotNull(database);
        checkState(ready);

        logger.info("Deleting local value for key {}", key);
        database.remove(key.getBytes());
        deleteCounter.inc();
    }

    @Override
    public void shutdown() {
        if (null != database) database.close();
        options.dispose();
    }

    @Override
    public boolean isReady() {
        return ready;
    }
}
