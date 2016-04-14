package me.tattsun.models;

import me.tattsun.AppConstants;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tattsun on 2016/04/14.
 */
public class Tweets {
    public static long newTweetId(long userId) throws IOException {
        Table table = DBConnection.getTable(AppConstants.TABLE_NAME);
        Increment increment = new Increment(Bytes.toBytes(userId));
        increment.addColumn(Bytes.toBytes(AppConstants.CF_TWEETS_COUNT),
                Bytes.toBytes(AppConstants.QUAL_TWEETS_COUNT), 1);
        Result result = table.increment(increment);
        return Bytes.toLong(result.value());
    }

    public static long getLatestTweetId(long userId) throws IOException {
        Table table = DBConnection.getTable(AppConstants.TABLE_NAME);
        Get get = new Get(Bytes.toBytes(userId));
        get.addColumn(Bytes.toBytes(AppConstants.CF_TWEETS_COUNT),
                Bytes.toBytes(AppConstants.QUAL_TWEETS_COUNT));
        Result result = table.get(get);
        return Bytes.toLong(result.value());
    }

    public static void addTweet(Tweet tweet) throws IOException {
        TraceScope ts = Trace.startSpan("addTweet", Sampler.ALWAYS);
        try {
            Table table = DBConnection.getTable(AppConstants.TABLE_NAME);

            long tweetId = newTweetId(tweet.userId);
            Put put = new Put(Bytes.toBytes(tweet.userId));
            put.addColumn(Bytes.toBytes(AppConstants.CF_TWEETS),
                    Bytes.toBytes(tweetId),
                    Bytes.toBytes(tweet.content));
            table.put(put);
        } finally {
            ts.close();
        }
    }

    public static List<Tweet> getTweets(long userId, long offset, long amount) throws IOException {
        TraceScope ts = Trace.startSpan("getTweets", Sampler.ALWAYS);

        try {
            Table table = DBConnection.getTable(AppConstants.TABLE_NAME);

            byte[] row = Bytes.toBytes(userId);

            long latestTweetId = getLatestTweetId(userId);
            long _limit = amount;
            long _offset = latestTweetId - offset - amount;
            if (_offset < 0) _offset = 0;

            Scan scan = new Scan(row, row);
            scan.addFamily(Bytes.toBytes(AppConstants.CF_TWEETS));
            Filter filter = new ColumnPaginationFilter((int) _limit, (int) _offset);
            scan.setFilter(filter);

            ResultScanner scanner = table.getScanner(scan);
            List<Tweet> tweets = new ArrayList<>((int) amount);
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    Tweet tweet = new Tweet(userId, Bytes.toString(cell.getValue()));
                    tweet.tweetId = Bytes.toLong(cell.getQualifier());
                    tweets.add(tweet);
                }
            }
            scanner.close();
            return tweets;
        } finally {
            ts.close();
        }
    }
}
