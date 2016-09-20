package org.jackliu.hbase.client.pools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * HBase connection pool key
 *
 * @author bing.liu
 * @date 2016-06-26
 */

class HConnectionKey {
    static final String[] CONNECTION_PROPERTIES = new String[]{"hbase.zookeeper.quorum", "zookeeper.znode.parent", "hbase.zookeeper.property.clientPort", "hbase.zookeeper.recoverable.waittime", "hbase.client.pause", "hbase.client.retries.number", "hbase.rpc.timeout", "hbase.meta.scanner.caching", "hbase.client.instance.id", "hbase.client.rpc.codec", "hbase.rpc.controllerfactory.class"};
    private Map<String, String> properties;
    private String username;

    HConnectionKey(Configuration conf) {
        HashMap m = new HashMap();
        if(conf != null) {
            String[] ioe = CONNECTION_PROPERTIES;
            int currentUser = ioe.length;

            for(int i$ = 0; i$ < currentUser; ++i$) {
                String property = ioe[i$];
                String value = conf.get(property);
                if(value != null) {
                    m.put(property, value);
                }
            }
        }

        this.properties = Collections.unmodifiableMap(m);

        try {
            UserProvider var9 = UserProvider.instantiate(conf);
            User var10 = var9.getCurrent();
            if(var10 != null) {
                this.username = var10.getName();
            }
        } catch (IOException var8) {
            HBaseConnectionProvider.LOGGER.warn("Error obtaining current user, skipping username in HConnectionKey", var8);
        }
    }

    public int hashCode() {
        boolean prime = true;
        int result = 1;
        if(this.username != null) {
            result = this.username.hashCode();
        }

        String[] arr$ = CONNECTION_PROPERTIES;
        int len$ = arr$.length;

        for(int i$ = 0; i$ < len$; ++i$) {
            String property = arr$[i$];
            String value = (String)this.properties.get(property);
            if(value != null) {
                result = 31 * result + value.hashCode();
            }
        }

        return result;
    }

    @SuppressWarnings(
        value = {"ES_COMPARING_STRINGS_WITH_EQ"}
    )
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        } else if(obj == null) {
            return false;
        } else if(this.getClass() != obj.getClass()) {
            return false;
        } else {
            HConnectionKey that = (HConnectionKey)obj;
            if(this.username != null && !this.username.equals(that.username)) {
                return false;
            } else if(this.username == null && that.username != null) {
                return false;
            } else {
                if(this.properties == null) {
                    if(that.properties != null) {
                        return false;
                    }
                } else {
                    if(that.properties == null) {
                        return false;
                    }

                    String[] arr$ = CONNECTION_PROPERTIES;
                    int len$ = arr$.length;

                    for(int i$ = 0; i$ < len$; ++i$) {
                        String property = arr$[i$];
                        String thisValue = (String)this.properties.get(property);
                        String thatValue = (String)that.properties.get(property);
                        if(thisValue != thatValue && (thisValue == null || !thisValue.equals(thatValue))) {
                            return false;
                        }
                    }
                }

                return true;
            }
        }
    }

    public String toString() {
        return "HConnectionKey{properties=" + this.properties + ", username=\'" + this.username + '\'' + '}';
    }
}
