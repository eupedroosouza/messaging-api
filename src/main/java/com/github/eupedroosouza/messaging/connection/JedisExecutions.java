package com.github.eupedroosouza.messaging.connection;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.JedisPubSub;

import java.nio.charset.StandardCharsets;

public interface JedisExecutions {

    long pub(String channel, String message);
    long pubBinary(byte[] channel, byte[] message);
    default long pubBinary(String channel, byte[] message) {
        return pubBinary(channel.getBytes(StandardCharsets.UTF_8), message);
    }

    void sub(JedisPubSub pubSub, String... channels);
    void subBinary(BinaryJedisPubSub pubSub, byte[]... channels);

}
