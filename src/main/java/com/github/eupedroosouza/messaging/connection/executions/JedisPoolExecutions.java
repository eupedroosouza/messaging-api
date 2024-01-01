/*
 * Copyright (c) 2024 Pedro Souza
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */

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
