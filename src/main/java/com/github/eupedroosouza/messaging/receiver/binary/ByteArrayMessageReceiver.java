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

package com.github.eupedroosouza.messaging.receiver.binary;

import com.github.eupedroosouza.messaging.connection.JedisExecutions;
import redis.clients.jedis.BinaryJedisPubSub;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

public abstract class ByteArrayMessageReceiver {

    private final BinaryJedisPubSub pubSub;
    private final Thread thread;

    public ByteArrayMessageReceiver(JedisExecutions executions, String channel) {
        this(executions, channel, (i) -> {}, (i) -> {});
    }

    public ByteArrayMessageReceiver(JedisExecutions executions, String channel, Consumer<Integer> onSubscribe, Consumer<Integer> onUnsubscribe) {
        this.pubSub = new BinaryJedisPubSub() {
            @Override
            public void onMessage(byte[] channel, byte[] message) {
                receive(message);
            }

            @Override
            public void onSubscribe(byte[] channel, int subscribedChannels) {
                onSubscribe.accept(subscribedChannels);
            }

            @Override
            public void onUnsubscribe(byte[] channel, int subscribedChannels) {
                onUnsubscribe.accept(subscribedChannels);
            }
        };
        thread = new Thread(() -> {
            executions.subBinary(pubSub, channel.getBytes(StandardCharsets.UTF_8));
        }, channel + "-receiver");
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        pubSub.unsubscribe();
        thread.interrupt();
    }

    public abstract void receive(byte[] message);

    public BinaryJedisPubSub getPubSub() {
        return pubSub;
    }

    public Thread getThread() {
        return thread;
    }
}
