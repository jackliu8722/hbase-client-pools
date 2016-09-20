package org.jackliu.hbase.client.pools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

/**
 * HBase connection warp
 *
 * @author bing.liu
 * @date 2016-05-26
 */
public class HBaseConnection {

    private Configuration conf;

    private Connection connection;

    public HBaseConnection(Configuration conf,Connection connection){
        this.conf = conf;
        this.connection = connection;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
