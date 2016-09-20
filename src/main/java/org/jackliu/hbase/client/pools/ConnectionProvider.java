package org.jackliu.hbase.client.pools;

import org.apache.hadoop.conf.Configuration;

/**
 * HBase connection provider
 *
 * @author bing.liu
 * @version 1.0
 */
public interface ConnectionProvider {

    /**
     *
     * @param conf
     * @return
     * @throws ConnectionPoolException
     */
    public HBaseConnection getConnection(Configuration conf) throws ConnectionPoolException;

    /**
     * Return the connection
     * @throws ConnectionPoolException
     */
    public void returnConnection(HBaseConnection conn) throws ConnectionPoolException;

    /**
     * Invalidate the connection
     * @param conn
     */
    public void invalidateConnection(HBaseConnection conn);

    /**
     * @param conf
     * Clear the connection given service node
     */
    public void clearConnections(Configuration conf);
}
