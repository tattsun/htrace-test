package me.tattsun.models;

/**
 * Created by tattsun on 2016/04/14.
 */
public class Tweet {
    public final long userId;
    public final String content;
    public long tweetId = 0;

    public Tweet(long userId, String content) {
        this.userId = userId;
        this.content = content;
    }
}
