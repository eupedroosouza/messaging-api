/*
 * Copyright � 2023 Pedro Souza
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the �Software�), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED �AS IS�, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.github.eupedroosouza.messaging.sender.binary;

import com.github.eupedroosouza.messaging.connection.BaseJedisConnection;
import com.github.eupedroosouza.messaging.connection.JedisConnectionProvider;
import com.github.eupedroosouza.messaging.data.DataKeys;
import com.github.eupedroosouza.messaging.exception.EmptyResponseException;
import com.github.eupedroosouza.messaging.message.MessageError;
import com.github.eupedroosouza.messaging.message.rpc.RPCByteArrayChannelResponse;
import com.github.eupedroosouza.messaging.message.status.MessageStatus;
import com.github.eupedroosouza.messaging.util.FutureUtil;
import com.github.eupedroosouza.messaging.util.GsonUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import redis.clients.jedis.BinaryJedisPubSub;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class RPCByteArrayChannelSender extends BaseJedisConnection {

    private final byte[] binarySenderChannel;
    private final BinaryJedisPubSub responseReceiverPubSub;
    private final Thread responseReceiverThread;

    private final HashMap<UUID, RPCByteArrayChannelResponse> messagesWaitingResponse = new HashMap<>();

    public RPCByteArrayChannelSender(JedisConnectionProvider connectionProvider, String channel,
                                     BiConsumer<String, Integer> onResponseChannelSubscribe, BiConsumer<String, Integer> onResponseChannelUnsubscribe) {
        super(connectionProvider);
        this.binarySenderChannel = (channel + ":sender").getBytes(StandardCharsets.UTF_8);
        byte[] binaryReceiverChannel = (channel + ":receiver").getBytes(StandardCharsets.UTF_8);
        this.responseReceiverPubSub = new BinaryJedisPubSub() {
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

                if (!messagesWaitingResponse.containsKey(correlationId))
                    return; // Handle this

                RPCByteArrayChannelResponse channelResponse = messagesWaitingResponse.get(correlationId);
                if (object.has(DataKeys.ERROR_CLASS_NAME_KEY) || object.has(DataKeys.ERROR_MESSAGE_KEY)) {
                    String errorClassName = object.has(DataKeys.ERROR_CLASS_NAME_KEY) ? object.get(DataKeys.ERROR_CLASS_NAME_KEY).getAsString() : null;
                    String errorMessage = object.has(DataKeys.ERROR_MESSAGE_KEY) ? object.get(DataKeys.ERROR_MESSAGE_KEY).getAsString() : null;
                    channelResponse.setStatus(MessageStatus.ERROR);
                    channelResponse.setError(new MessageError(errorClassName, errorMessage));
                    channelResponse.getWaitingResponse().complete(null);
                    return;
                }

                if (!object.has(DataKeys.RESPONSE_KEY)) {
                    channelResponse.getWaitingResponse().completeExceptionally(new EmptyResponseException("Empty response received"));
                    return;
                }

                try {
                    channelResponse.setResponse(Base64.getDecoder().decode(object.get(DataKeys.RESPONSE_KEY).getAsString()));
                    channelResponse.setStatus(MessageStatus.SUCCESS);
                    channelResponse.getWaitingResponse().complete(null);
                } catch (IllegalArgumentException ex) {
                    channelResponse.getWaitingResponse().completeExceptionally(ex);
                }
            }

            @Override
            public void onSubscribe(byte[] channel, int subscribedChannels) {
                onResponseChannelSubscribe.accept(new String(channel, StandardCharsets.UTF_8), subscribedChannels);
            }

            @Override
            public void onUnsubscribe(byte[] channel, int subscribedChannels) {
                onResponseChannelUnsubscribe.accept(new String(channel, StandardCharsets.UTF_8), subscribedChannels);
            }
        };
        this.responseReceiverThread = new Thread(() -> {
            subBinary(responseReceiverPubSub, binaryReceiverChannel);
        }, channel + "-response-receiver");
    }

    public void start() {
        responseReceiverThread.start();
    }

    public void shutdown() {
        responseReceiverPubSub.unsubscribe();
        responseReceiverThread.interrupt();
    }

    public CompletableFuture<RPCByteArrayChannelResponse> send(byte[] message) {
        return send(message, 0, 0);
    }

    public CompletableFuture<RPCByteArrayChannelResponse> send(byte[] message, long timeout) {
        return send(message, timeout, 0);
    }

    public CompletableFuture<RPCByteArrayChannelResponse> sendWithRemoteTimeout(byte[] message, long remoteTimeout) {
        return send(message, 0, remoteTimeout);
    }

    public CompletableFuture<RPCByteArrayChannelResponse> send(byte[] message, long timeout, long remoteTimeout) {
        return FutureUtil.exceptionAsyncFuture(() -> {
            UUID correlationId = generateCorrelationId();
            RPCByteArrayChannelResponse channelResponse = new RPCByteArrayChannelResponse();
            messagesWaitingResponse.put(correlationId, channelResponse);
            try {
                JsonObject object = new JsonObject();
                object.addProperty(DataKeys.CORRELATION_ID_KEY, correlationId.toString());
                object.addProperty(DataKeys.MESSAGE_KEY, Base64.getEncoder().encodeToString(message));
                object.addProperty(DataKeys.REMOTE_TIMEOUT_KEY, remoteTimeout);
                long status = pubBinary(binarySenderChannel, GsonUtil.GSON.toJson(object).getBytes(StandardCharsets.UTF_8));
                if (status == 0)  {
                    channelResponse.setStatus(MessageStatus.NOT_SUBSCRIBERS_CHANNEL);
                    return channelResponse;
                }
                if (timeout > 0)
                    channelResponse.getWaitingResponse().get(timeout, TimeUnit.MILLISECONDS);
                else channelResponse.getWaitingResponse().get();
                return channelResponse;
            } finally {
                messagesWaitingResponse.remove(correlationId);
            }
        });
    }

    private UUID generateCorrelationId() {
        UUID generatedCorrelationId;
        do {
            generatedCorrelationId = UUID.randomUUID();
        } while (messagesWaitingResponse.containsKey(generatedCorrelationId));
        return generatedCorrelationId;
    }

}
