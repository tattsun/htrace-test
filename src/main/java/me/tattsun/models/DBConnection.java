package me.tattsun.models;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.trace.SpanReceiverHost;

import java.io.IOException;
import java.util.Map;

/**
 * Created by tattsun on 2016/04/14.
 */
class DBConnection {
    private static Connection connection;

    static {
        Configuration conf = new Configuration();
        conf.set("hbase.trace.spanreceiver.classes", "org.apache.htrace.impl.ZipkinSpanReceiver");
        conf.set("hbase.htrace.zipkin.collector-hostname", "192.168.99.100");
        conf.set("hbase.htrace.zipkin.collector-port", "9410");
        SpanReceiverHost.getInstance(conf);

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            System.out.println("DBConnection initialize failed");
            System.out.println(e.toString());
            System.exit(1);
        }
    }

    static Table getTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }
}
