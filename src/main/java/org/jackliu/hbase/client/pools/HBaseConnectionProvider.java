package org.jackliu.hbase.client.pools;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TooManyListenersException;

/**
 * The manager class of the connection pool
 *
 * @author bing.liu
 * @date 2016-05-26
 * @version 1.0
 */
public class HBaseConnectionProvider implements ConnectionProvider{

    /** Logger */
    public static final Logger LOGGER = LoggerFactory.getLogger(HBaseConnectionProvider.class);

    /** The max number of the active object in the pool*/
    private static int maxActive = 200;

    /** The max number of the idle object in the pool*/
    private static int maxIdle = 40;

    /** The min number of the idle object in the pool*/
    private static int minIdle = 10;

    /** The max wait time (ms)*/
    private static long maxWait = 10;

    /** Weather execute PoolableObjectFactory.validateObject method
     * when allocate object from the pool*/
    private boolean testOnBorrow = false;

    /** Weather execute PoolableObjectFactory.validateObject method
     * when return object */
    private boolean testOnReturn = false;

    private boolean testWhileIdel = false;

    private static HashMap<HConnectionKey,ObjectPool> poolMap = new HashMap<HConnectionKey, ObjectPool>();

    public HBaseConnectionProvider(){

    }

    /**
     * Set the pool parameter
     *
     * @param maxActive
     * @param maxIdle
     * @param minIdle
     * @param maxWait
     */
    public static void setPoolParam(int maxActive,int maxIdle,int minIdle,int maxWait){
        HBaseConnectionProvider.maxActive = maxActive;
        HBaseConnectionProvider.maxIdle = maxIdle;
        HBaseConnectionProvider.minIdle = minIdle;
        HBaseConnectionProvider.maxWait = maxWait;
    }

    /**
     * Create the pool given the Configuration
     * @param conf
     * @return
     */
    ObjectPool createPool(Configuration conf){
        /** Create factory*/
        HBaseConectionPoolableObjectFactory poolableObjectFactory =
                new HBaseConectionPoolableObjectFactory(conf);

        GenericObjectPool objectPool = new GenericObjectPool(poolableObjectFactory);

        objectPool.setMaxActive(maxActive);
        objectPool.setMaxIdle(maxIdle);
        objectPool.setMinIdle(minIdle);
        objectPool.setMaxWait(maxWait);

        objectPool.setTestOnBorrow(testOnBorrow);
        objectPool.setTestOnReturn(testOnReturn);
        objectPool.setTestWhileIdle(testWhileIdel);

        /** borrowObject method will lock when there is no object in the pool*/
        objectPool.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_FAIL);

        return objectPool;
    }

    /**
     * Get the pool connection status
     * @return
     */
    public static String getConnStatus(){
        StringBuilder message = new StringBuilder();
        for(Map.Entry<?,?> entry : poolMap.entrySet()){
            HConnectionKey key = (HConnectionKey) entry.getKey();
            ObjectPool pool = (ObjectPool) entry.getValue();

            message.append("Status of connection [").append(key).append("] is:\n");
            message.append("\tpool using size:").append(pool.getNumActive()).append("\n");
            message.append("\tpool idle size:").append(pool.getNumIdle()).append("\n");
        }
        return message.toString();
    }

    /**
     * Get a connection
     * @param conf
     * @return
     * @throws ConnectionPoolException
     */
    @Override
    public HBaseConnection getConnection(Configuration conf) throws ConnectionPoolException {
        Connection conn = null;
        ObjectPool pool = null;
        HConnectionKey key = new HConnectionKey(conf);
        try{
            pool = poolMap.get(key);
            if(pool == null){
                synchronized (key.toString().intern()){
                    if(!poolMap.containsKey(key)){
                        pool = createPool(conf);
                        poolMap.put(key,pool);
                        LOGGER.info("Pool construction " + key);
                    }else{
                        pool = poolMap.get(key);
                    }
                }
            }

            conn = (Connection) pool.borrowObject();
            if(LOGGER.isDebugEnabled()){
                LOGGER.debug("Pool-stat: getConnection at key=" + key + ", active=" + pool.getNumActive()
                    + ",idle=" + pool.getNumIdle());
            }
            return new HBaseConnection(conf,conn);
        }catch (java.util.NoSuchElementException e){
            String msg = "Client pool exhausted and cannot or will not return another instance : " + key +
                 ",active=" + pool.getNumActive() + ",idle=" + pool.getNumIdle();
            throw new ConnectionPoolException(msg,e);
        }catch (IllegalStateException e){
            throw new ConnectionPoolException("Client pool you called has been closed.",e);
        }catch (Exception e){
            throw new ConnectionPoolException("Client pool other exception at " + key + "," + e.getMessage(),e );
        }
    }

    /**
     * Return a connection
     * @param conn
     * @throws TooManyListenersException
     */
    @Override
    public void returnConnection(HBaseConnection conn) throws ConnectionPoolException {
        HConnectionKey key = new HConnectionKey(conn.getConf());
        ObjectPool pool = null;

        try{
            pool = poolMap.get(key);
            if(pool != null){
                if(conn.getConnection() != null){
                    pool.returnObject(conn.getConnection());
                }
            }else{
                if(LOGGER.isDebugEnabled()){
                    LOGGER.debug("Pool-stat: returnConnection " + conn.getConnection() +
                            ", pool key : " + key + ", pool not exist.");
                    LOGGER.debug("servercPoolMap : " + poolMap);
                }
            }
        }catch (Exception e){
            throw new ConnectionPoolException("Return connction error at key=" + key,e);
        }
    }


    /**
     * Invalidate connection
     * @param conn
     */
    @Override
    public void invalidateConnection(HBaseConnection conn){
        HConnectionKey key = new HConnectionKey(conn.getConf());
        Connection connection = conn.getConnection();
        ObjectPool pool = null;

        try{
            pool = poolMap.get(key);
            if(pool != null){
                if(connection != null){
                    pool.invalidateObject(connection);
                }
                if(LOGGER.isDebugEnabled()){
                    LOGGER.debug("Pool-stat: invalidate " + connection + ",active=" + pool.getNumActive()
                        + ",idle=" + pool.getNumIdle());
                }
            }
        }catch (Exception e){
            LOGGER.warn("InvalidateConnection error.",e);
        }
    }

    @Override
    public void clearConnections(Configuration conf){
        HConnectionKey key = new HConnectionKey(conf);
        ObjectPool pool = null;

        try{
            pool = poolMap.get(key);
            if(pool != null){
                pool.clear();
            }
        }catch (Exception e){
            LOGGER.warn("ClearConnection error at key=" + key,e);
        }
        LOGGER.info("Pool-stat: pool destruction at key=" + key);
    }

    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    public void setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }

    public boolean isTestWhileIdel() {
        return testWhileIdel;
    }

    public void setTestWhileIdel(boolean testWhileIdel) {
        this.testWhileIdel = testWhileIdel;
    }
}
