/*
 * Copyright � 2023 Pedro Souza
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the �Software�), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED �AS IS�, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.github.eupedroosouza.messaging.connection;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.JedisPubSub;

import java.nio.charset.StandardCharsets;

public class BaseJedisConnection {

    private final JedisConnectionProvider connectionProvider;

    public BaseJedisConnection(JedisConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public long pub(String channel, String message) {
        return connectionProvider.getConnection().publish(channel, message);
    }

    public long pubBinary(String channel, byte[] message) {
        return pubBinary(channel.getBytes(StandardCharsets.UTF_8), message);
    }

    public long pubBinary(byte[] channel, byte[] message) {
        return connectionProvider.getConnection().publish(channel, message);
    }

    public void sub(JedisPubSub pubSub, String... channels) {
        connectionProvider.getConnection().subscribe(pubSub, channels);
    }

    public void subBinary(BinaryJedisPubSub pubSub, String... channels) {
        byte[] binaryChannels = new byte[channels.length];
        for (int i = 0; i < channels.length; i++) {
            binaryChannels[i] = channels[i].getBytes(StandardCharsets.UTF_8)[0];
        }
        subBinary(pubSub, binaryChannels);
    }

    public void subBinary(BinaryJedisPubSub pubSub, byte[]... channels) {
        connectionProvider.getConnection().subscribe(pubSub, channels);
    }

}
