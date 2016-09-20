package org.jackliu.hbase.client.pools.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jackliu.hbase.client.pools.ConnectionPoolException;
import org.jackliu.hbase.client.pools.ConnectionProvider;
import org.jackliu.hbase.client.pools.HBaseConnection;
import org.jackliu.hbase.client.pools.HBaseConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * The implementation of the store used HBase
 *
 * @author bing.liu
 * @date 2016-05-23
 */
public class Example {

    /** size column */
    private static final String SIZE_COLUMN = "size";

    private static final Logger LOGGER = LoggerFactory.getLogger(Example.class);

    private Configuration config;

    private String tableName;

    private String familyName;

    private ConnectionProvider provider = new HBaseConnectionProvider();

    public Example(){
        this.config = HBaseConfiguration.create();
        this.config.set("hbase.zookeeper.quorum", "zk1,zk2,zk3");
        this.config.set("hbase.zookeeper.property.clientPort","2181");
        this.config.set("zookeeper.znode.parent","/hbase");
        this.tableName = "test";
        this.familyName = "f1";
    }


    public void put(){

        HBaseConnection conn;
        try{
            conn = provider.getConnection(this.config);
        }catch (ConnectionPoolException e){
            if(LOGGER.isDebugEnabled()){
                LOGGER.debug("Get connection error.",e);
            }
            return ;
        }
        Table table = null;
        try {
            table = conn.getConnection().getTable(TableName.valueOf(this.tableName));

            Increment ii = new Increment(Bytes.toBytes("key"));
            ii.setTTL(7 * 24 * 60 * 60 * 1000);
            ii.addColumn(Bytes.toBytes(this.familyName), Bytes.toBytes("c1"), 1);
            table.increment(ii);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    LOGGER.warn("Table close encounter exception.",e);
                }
            }
            try {
                provider.returnConnection(conn);
            } catch (ConnectionPoolException e) {
                if(LOGGER.isDebugEnabled()){
                    LOGGER.debug("Connection return error.",e);
                }
            }
        }
    }


    public void get(){

        HBaseConnection conn;
        try{
            conn = provider.getConnection(this.config);
        }catch (ConnectionPoolException e){
            if(LOGGER.isDebugEnabled()){
                LOGGER.debug("Get connection error.",e);
            }
            return;
        }
        Table table = null;
        try {
            table = conn.getConnection().getTable(TableName.valueOf(this.tableName));

            Get g = new Get(Bytes.toBytes("key"));
            g.addColumn(Bytes.toBytes(this.familyName),Bytes.toBytes("c1"));

            Result result = table.get(g);

            Map<byte[], byte[]> m = result.getFamilyMap(Bytes.toBytes(this.familyName));
            if(m != null) {
                for (Map.Entry<byte[], byte[]> entry : m.entrySet()) {
                    System.out.println("column = " + Bytes.toString(entry.getKey()) + ", value = " + Bytes.toLong(entry.getValue()));
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    LOGGER.warn("Table close encounter exception.",e);
                }
            }
            try {
                provider.returnConnection(conn);
            } catch (ConnectionPoolException e) {
                if(LOGGER.isDebugEnabled()){
                    LOGGER.debug("Connection return error.",e);
                }
            }
        }
    }


    public static void main(String []args) {
        Example e = new Example();
        e.put();
        e.get();
    }

}
