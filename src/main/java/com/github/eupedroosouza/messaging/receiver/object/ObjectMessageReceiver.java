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

package com.github.eupedroosouza.messaging.receiver.object;

import com.github.eupedroosouza.messaging.connection.JedisExecutions;
import com.github.eupedroosouza.messaging.data.DataKeys;
import com.github.eupedroosouza.messaging.exception.ChannelException;
import com.github.eupedroosouza.messaging.message.MessageObject;
import com.github.eupedroosouza.messaging.util.GsonUtil;
import com.github.eupedroosouza.messaging.util.ObjectMessageUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import redis.clients.jedis.JedisPubSub;

import java.util.function.Consumer;

public abstract class ObjectMessageReceiver {

    private final JedisPubSub pubSub;
    private final Thread thread;

    public ObjectMessageReceiver(JedisExecutions executions, String channel) {
        this(executions, channel, (i) -> {}, (i) -> {});
    }

    public ObjectMessageReceiver(JedisExecutions executions, String channel, Consumer<Integer> onSubscribe, Consumer<Integer> onUnsubscribe) {
        this.pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                JsonObject object;
                try {
                    object = GsonUtil.GSON.fromJson(message, JsonObject.class);
                } catch (JsonParseException ex) {
                    throw ex; // Handle the exception
                }

                if (!object.has(DataKeys.CLASS_NAME_KEY))
                    return; // Handle this

                if (!object.has(DataKeys.MESSAGE_KEY))
                    return; // Handle this

                String className = object.get(DataKeys.CLASS_NAME_KEY).getAsString();
                JsonObject messageReceived = object.get(DataKeys.MESSAGE_KEY).getAsJsonObject();
                try {
                    Class<? extends MessageObject> clazz = Class.forName(className).asSubclass(MessageObject.class);
                    receive((MessageObject) ObjectMessageUtil.deserialize(clazz, messageReceived));
                } catch (ClassNotFoundException ex) {
                    throw new ChannelException("Class " + className + " of received message not found", ex);  // Handle the exception
                } catch (ClassCastException ex) {
                    throw new ChannelException("The class " + className + " is not assignable from MessageObject", ex); // Handle the exception
                } catch(Exception ex) {
                    throw ex; // Handle the exception
                }
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                onSubscribe.accept(subscribedChannels);
            }

            @Override
            public void onUnsubscribe(String channel, int subscribedChannels) {
                onUnsubscribe.accept(subscribedChannels);
            }
        };
        this.thread = new Thread(() -> {
            executions.sub(pubSub, channel);
        });
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        pubSub.unsubscribe();
        thread.interrupt();
    }

    public abstract <T extends MessageObject> void receive(T messageObject);

}
