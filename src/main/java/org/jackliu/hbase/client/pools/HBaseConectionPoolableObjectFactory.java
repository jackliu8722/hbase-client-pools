package org.jackliu.hbase.client.pools;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PoolableObjectFactory implement
 *
 * @author bing.liu
 * @date 2015-08-05
 * @version 1.0
 */
public class HBaseConectionPoolableObjectFactory implements PoolableObjectFactory{

    /** Logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConectionPoolableObjectFactory.class);

    /** HBase configuration */
    private Configuration conf;

    public HBaseConectionPoolableObjectFactory(Configuration conf){
        this.conf = conf;
    }

    /**
     * Destroy connection
     *
     * @param arg0
     * @throws Exception
     */
    @Override
    public void destroyObject(Object arg0) throws Exception{
        if(arg0 instanceof Connection){
            Connection conn = (Connection) arg0;
            if(!conn.isClosed()){
                conn.close();
            }
        }
    }

    /**
     * Make a connection
     * @return
     * @throws Exception
     */
    @Override
    public Object makeObject() throws Exception{
        try{
            Connection conn = ConnectionFactory.createConnection(this.conf);
            return conn;
        }catch (Exception e){
            LOGGER.warn("Pool make hbase connection error.");
            throw new RuntimeException(e);
        }
    }

    /**
     * Validate the connection
     * @param arg0
     * @return
     */
    @Override
    public boolean validateObject(Object arg0){
        try{
            if(arg0 instanceof Connection){
                Connection conn = (Connection) arg0;

                if(conn.isClosed()){
                    return false;
                }else{
                    return true;
                }
            }else{
                return false;
            }
        }catch (Exception e){
            return false;
        }
    }

    @Override
    public void passivateObject(Object arg0) throws Exception{
        //Do nothing
    }

    @Override
    public void activateObject(Object arg0) throws Exception {
        // Do nothing
    }

    public Configuration getConf(){
        return conf;
    }
}
