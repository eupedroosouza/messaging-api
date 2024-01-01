package com.github.eupedroosouza.messaging.connection.executions;

import com.github.eupedroosouza.messaging.connection.JedisExecutions;
import redis.clients.jedis.*;

public class JedisPoolExecutions implements JedisExecutions {

    private final JedisPool pool;

    public JedisPoolExecutions(JedisPool pool) {
        this.pool = pool;
    }

    @Override
    public long pub(String channel, String message) {
        try (Jedis connection = pool.getResource()) {
            return connection.publish(channel, message);
        }
    }

    @Override
    public long pubBinary(byte[] channel, byte[] message) {
        try (Jedis connection = pool.getResource()) {
            return connection.publish(channel, message);
        }
    }

    @Override
    public void sub(JedisPubSub pubSub, String... channels) {
        try (Jedis connection = pool.getResource()) {
            connection.subscribe(pubSub, channels);
        }
    }

    @Override
    public void subBinary(BinaryJedisPubSub pubSub, byte[]... channels) {
        try (Jedis connection = pool.getResource()) {
            connection.subscribe(pubSub, channels);
        }
    }

}
