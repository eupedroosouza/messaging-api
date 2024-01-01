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
import com.github.eupedroosouza.messaging.message.MessageObject;
import com.github.eupedroosouza.messaging.util.FutureUtil;
import com.github.eupedroosouza.messaging.util.GsonUtil;
import com.github.eupedroosouza.messaging.util.ObjectMessageUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import redis.clients.jedis.JedisPubSub;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public abstract class RPCObjectMessageReceiver {

    private final JedisExecutions executions;
    private final String receiverChannel;

    private final JedisPubSub receiverPubSub;
    private final Thread receiverThread;

    public RPCObjectMessageReceiver(JedisExecutions executions, String channel) {
        this(executions, channel, (c, sc) -> {}, (c, sc) -> {});
    }

    public RPCObjectMessageReceiver(JedisExecutions executions, String channel, BiConsumer<String, Integer> onReceiverSubscribe,
                                    BiConsumer<String, Integer> onReceiverUnsubscribe) {
        this.executions = executions;
        String senderChannel = (channel + ":sender");
        this.receiverChannel = (channel + ":receiver");
        this.receiverPubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                JsonObject object;
                try {
                    object = GsonUtil.GSON.fromJson(message, JsonObject.class);
                } catch (JsonParseException ex) {
                    throw ex; // Handle the exception
                }

                if (!object.has(DataKeys.CORRELATION_ID_KEY))
                    return; // Handle this
                UUID correlationId;
                try {
                    correlationId = UUID.fromString(object.get(DataKeys.CORRELATION_ID_KEY).getAsString());
                } catch (IllegalArgumentException ex) {
                    throw ex; // Handle the exception
                }

                JsonObject responseObject = new JsonObject();
                responseObject.addProperty(DataKeys.CORRELATION_ID_KEY, correlationId.toString());

                if (!object.has(DataKeys.MESSAGE_KEY)) {
                    responseObject.addProperty(DataKeys.ERROR_MESSAGE_KEY, "Empty message received");
                    send(responseObject);
                    return;
                }

                JsonObject messageReceived = object.get(DataKeys.MESSAGE_KEY).getAsJsonObject();
                String className = messageReceived.get(DataKeys.CLASS_NAME_KEY).getAsString();
                long timeout = messageReceived.has(DataKeys.REMOTE_TIMEOUT_KEY) ? messageReceived.get(DataKeys.REMOTE_TIMEOUT_KEY).getAsLong() : 0;
                FutureUtil.exceptionAsyncFuture(() -> {
                    Class<? extends MessageObject> clazz = Class.forName(className).asSubclass(MessageObject.class);
                    Object o = ObjectMessageUtil.deserialize(clazz, messageReceived);
                    CompletableFuture<? extends MessageObject> future = receive((MessageObject) o);
                    Object response = timeout > 0 ?
                            future.get(timeout, TimeUnit.MILLISECONDS) :
                            future.get();

                    responseObject.addProperty(DataKeys.CLASS_NAME_KEY, response.getClass().getCanonicalName());
                    responseObject.add(DataKeys.RESPONSE_KEY, ((MessageObject)response).serialize());
                    send(responseObject);
                    return null;
                }).whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        responseObject.addProperty(DataKeys.ERROR_CLASS_NAME_KEY, throwable.getClass().getName());
                        responseObject.addProperty(DataKeys.ERROR_MESSAGE_KEY, throwable.getMessage());
                        send(responseObject);
                    }

                });
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                onReceiverSubscribe.accept(channel, subscribedChannels);
            }

            @Override
            public void onUnsubscribe(String channel, int subscribedChannels) {
                onReceiverUnsubscribe.accept(channel, subscribedChannels);
            }
        };
        this.receiverThread = new Thread(() -> {
            executions.sub(receiverPubSub, receiverChannel);
        }, channel + "-receiver");
    }

    public void start() {
        receiverThread.start();
    }

    public void shutdown() {
        receiverPubSub.unsubscribe();
        receiverThread.interrupt();
    }

    private void send(JsonObject object) {
        executions.pub(receiverChannel, GsonUtil.GSON.toJson(object));
    }

    public abstract <T extends MessageObject> CompletableFuture<? extends MessageObject> receive(T messageObject);

}
