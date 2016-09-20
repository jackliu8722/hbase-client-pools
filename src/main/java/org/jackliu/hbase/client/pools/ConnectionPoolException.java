package org.jackliu.hbase.client.pools;

/**
 * HBase connection pool exception
 *
 * @author bing.liu
 * @date 2016-06-26
 * @version 1.0
 */
public class ConnectionPoolException extends Exception{

    public ConnectionPoolException(String s){
        super(s);
    }

    public ConnectionPoolException(Throwable e){
        super(e);
    }

    public ConnectionPoolException(String s, Throwable e){
        super(s,e);
    }
}
