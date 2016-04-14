package me.tattsun.controllers;

import com.google.common.collect.Maps;
import me.tattsun.models.Tweet;
import me.tattsun.models.Tweets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.trace.SpanReceiverHost;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Map;

/**
 * Created by tattsun on 2016/04/14.
 */
@RestController
public class TweetController {
    @RequestMapping(value = "tweets", method = RequestMethod.GET)
    public Map<String, Object> getTweets(@RequestParam(value="userId") long userId,
                                         @RequestParam(value="offset", defaultValue="0") long offset,
                                         @RequestParam(value="amount", defaultValue="10") long amount) throws IOException {
        Map<String, Object> map = Maps.newHashMap();
        map.put("tweets", Tweets.getTweets(userId, offset, amount));
        return map;
    }

    @RequestMapping(value = "tweets", method = RequestMethod.POST)
    public Map<String, String> addTweets(@RequestParam(value="userId") long userId,
                                         @RequestParam(value="content") String content) throws IOException {
        Tweets.addTweet(new Tweet(userId, content));

        Map<String, String> map = Maps.newHashMap();
        map.put("status", "ok");
        return map;
    }
}
