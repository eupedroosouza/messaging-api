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
