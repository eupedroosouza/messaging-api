/*
 * Copyright � 2023 Pedro Souza
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the �Software�), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED �AS IS�, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.github.eupedroosouza.messaging.receiver.binary;

import com.github.eupedroosouza.messaging.connection.BaseJedisConnection;
import com.github.eupedroosouza.messaging.connection.JedisConnectionProvider;
import com.github.eupedroosouza.messaging.data.DataKeys;
import com.github.eupedroosouza.messaging.util.FutureUtil;
import com.github.eupedroosouza.messaging.util.GsonUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import redis.clients.jedis.BinaryJedisPubSub;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

public abstract class RPCByteArrayMessageReceiver extends BaseJedisConnection {

    private final byte[] binarySenderChannel;
    private final byte[] binaryReceiverChannel;
    private final BinaryJedisPubSub receiverPubSub;
    private final Thread receiverThread;

    public RPCByteArrayMessageReceiver(JedisConnectionProvider connectionProvider, String channel) {
        this(connectionProvider, channel, (s, i) -> {}, (s, i) -> {});
    }

    public RPCByteArrayMessageReceiver(JedisConnectionProvider connectionProvider, String channel,
                                       BiConsumer<String, Integer> onReceiverSubscribe, BiConsumer<String, Integer> onReceiverUnsubscribe) {
        super(connectionProvider);
        this.binarySenderChannel = (channel + ":sender").getBytes(StandardCharsets.UTF_8);
        this.binaryReceiverChannel = (channel + ":receiver").getBytes(StandardCharsets.UTF_8);
        this.receiverPubSub = new BinaryJedisPubSub() {
            @Override
            public void onMessage(byte[] channel, byte[] message) {
                JsonObject object;
                try {
                    object = GsonUtil.GSON.fromJson(new String(message, StandardCharsets.UTF_8), JsonObject.class);
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

                byte[] messageReceived;
                try {
                    messageReceived = Base64.getDecoder().decode(object.get(DataKeys.MESSAGE_KEY).getAsString());
                } catch (IllegalArgumentException ex) {
                    responseObject.addProperty(DataKeys.ERROR_CLASS_NAME_KEY, ex.getClass().getCanonicalName());
                    responseObject.addProperty(DataKeys.ERROR_MESSAGE_KEY, ex.getMessage());
                    send(responseObject);
                    return;
                }

                long remoteTimeout = object.has(DataKeys.REMOTE_TIMEOUT_KEY) ? object.get(DataKeys.REMOTE_TIMEOUT_KEY).getAsLong() : 0;
                FutureUtil.exceptionAsyncFuture(() -> {
                    byte[] response = remoteTimeout > 0 ?
                            receive(messageReceived).get(remoteTimeout, TimeUnit.MILLISECONDS) :
                            receive(messageReceived).get();

                    responseObject.addProperty(DataKeys.RESPONSE_KEY, Base64.getEncoder().encodeToString(response));
                    send(responseObject);
                    return null;
                }).whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        responseObject.addProperty(DataKeys.ERROR_CLASS_NAME_KEY, throwable.getClass().getCanonicalName());
                        responseObject.addProperty(DataKeys.ERROR_MESSAGE_KEY, throwable.getMessage());
                        send(responseObject);
                    }
                });

            }

            @Override
            public void onSubscribe(byte[] channel, int subscribedChannels) {
                onReceiverSubscribe.accept(new String(channel, StandardCharsets.UTF_8), subscribedChannels);
            }

            @Override
            public void onUnsubscribe(byte[] channel, int subscribedChannels) {
                onReceiverUnsubscribe.accept(new String(channel, StandardCharsets.UTF_8), subscribedChannels);
            }
        };
        this.receiverThread = new Thread(() -> {
            subBinary(receiverPubSub, binarySenderChannel);
        }, new String(binarySenderChannel, StandardCharsets.UTF_8) + "-receiver");
    }

    public void start() {
        receiverThread.start();
    }

    public void shutdown() {
        receiverPubSub.unsubscribe();
        receiverThread.interrupt();
    }

    private void send(JsonObject object) {
        pubBinary(binaryReceiverChannel, GsonUtil.GSON.toJson(object).getBytes(StandardCharsets.UTF_8));
    }

    public abstract CompletableFuture<byte[]> receive(byte[] message);

}
