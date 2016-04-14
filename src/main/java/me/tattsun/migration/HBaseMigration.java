package me.tattsun.migration;

import me.tattsun.AppConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by tattsun on 2016/04/14.
 */
public class HBaseMigration {
    public static void migrate() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor descriptor = new HTableDescriptor(AppConstants.TABLE_NAME);
        descriptor.addFamily(new HColumnDescriptor(AppConstants.CF_TWEETS));
        descriptor.addFamily(new HColumnDescriptor(AppConstants.CF_TWEETS_COUNT));
        admin.createTable(descriptor);
    }
}
