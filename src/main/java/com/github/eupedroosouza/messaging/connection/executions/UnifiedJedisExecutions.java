package com.github.eupedroosouza.messaging.connection.executions;

import com.github.eupedroosouza.messaging.connection.JedisExecutions;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.UnifiedJedis;

public class UnifiedJedisExecutions implements JedisExecutions {

    private final UnifiedJedis unifiedJedis;

    public UnifiedJedisExecutions(UnifiedJedis unifiedJedis) {
        this.unifiedJedis = unifiedJedis;
    }

    @Override
    public long pub(String channel, String message) {
        return unifiedJedis.publish(channel, message);
    }

    @Override
    public long pubBinary(byte[] channel, byte[] message) {
        return unifiedJedis.publish(channel, message);
    }

    @Override
    public void sub(JedisPubSub pubSub, String... channels) {
        unifiedJedis.subscribe(pubSub, channels);
    }

    @Override
    public void subBinary(BinaryJedisPubSub pubSub, byte[]... channels) {
        unifiedJedis.subscribe(pubSub, channels);
    }
}
