/*
 * Copyright � 2023 Pedro Souza
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the �Software�), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED �AS IS�, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.github.eupedroosouza.messaging.sender.object;

import com.github.eupedroosouza.messaging.connection.BaseJedisConnection;
import com.github.eupedroosouza.messaging.connection.JedisConnectionProvider;
import com.github.eupedroosouza.messaging.data.DataKeys;
import com.github.eupedroosouza.messaging.exception.ChannelException;
import com.github.eupedroosouza.messaging.message.MessageError;
import com.github.eupedroosouza.messaging.message.MessageObject;
import com.github.eupedroosouza.messaging.message.rpc.RPCObjectChannelResponse;
import com.github.eupedroosouza.messaging.message.status.MessageStatus;
import com.github.eupedroosouza.messaging.util.FutureUtil;
import com.github.eupedroosouza.messaging.util.GsonUtil;
import com.github.eupedroosouza.messaging.util.ObjectMessageUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import redis.clients.jedis.JedisPubSub;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class RPCObjectMessageSender extends BaseJedisConnection {

    private static final Method SET_RESPONSE_METHOD;

    static {
        try {
            SET_RESPONSE_METHOD = RPCObjectChannelResponse.class.getMethod("setResponse", MessageObject.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Method setResponse no such.", e);
        }
    }

    private final String senderChannel;
    private final JedisPubSub responseReceiverPubSub;
    private final Thread responseReceiverThread;

    private final HashMap<UUID, RPCObjectChannelResponse<?>> messagesWaitingResponse = new HashMap<>();

    public RPCObjectMessageSender(JedisConnectionProvider connectionProvider, String channel) {
        this(connectionProvider, channel, (c, sc) -> {}, (c, sc) -> {});
    }

    public RPCObjectMessageSender(JedisConnectionProvider connectionProvider, String channel, BiConsumer<String, Integer> onResponseChannelSubscribe,
                                  BiConsumer<String, Integer> onResponseChannelUnsubscribe) {
        super(connectionProvider);
        this.senderChannel = (channel + ":sender");
        String receiverChannel = (channel + ":receiver");
        this.responseReceiverPubSub = new JedisPubSub() {
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

                if (!messagesWaitingResponse.containsKey(correlationId))
                    return; // Handle this
                RPCObjectChannelResponse<? extends MessageObject> channelResponse = messagesWaitingResponse.get(correlationId);
                if (object.has(DataKeys.ERROR_CLASS_NAME_KEY) || object.has(DataKeys.ERROR_MESSAGE_KEY)) {
                    String errorClassName = object.has(DataKeys.ERROR_CLASS_NAME_KEY) ? object.get(DataKeys.ERROR_CLASS_NAME_KEY).getAsString() : null;
                    String errorMessage = object.has(DataKeys.ERROR_MESSAGE_KEY) ? object.get(DataKeys.ERROR_MESSAGE_KEY).getAsString() : null;
                    channelResponse.setStatus(MessageStatus.ERROR);
                    channelResponse.setError(new MessageError(errorClassName, errorMessage));
                    channelResponse.getWaitingResponse().complete(null);
                    return;
                }

                if (!object.has(DataKeys.CLASS_NAME_KEY))
                    return; // Handle this

                if (!object.has(DataKeys.RESPONSE_KEY))
                    return; // Handle this

                String className = object.get(DataKeys.CLASS_NAME_KEY).getAsString();
                JsonObject response = object.get(DataKeys.RESPONSE_KEY).getAsJsonObject();
                try {
                    Class<? extends MessageObject> clazz = Class.forName(className).asSubclass(MessageObject.class);
                    Object o = ObjectMessageUtil.deserialize(clazz, response);
                    SET_RESPONSE_METHOD.invoke(channelResponse, clazz.cast(o));
                    channelResponse.setStatus(MessageStatus.SUCCESS);
                    channelResponse.getWaitingResponse().complete(null);
                } catch(Exception ex) {
                    Throwable throwable;
                    if (ex instanceof ClassNotFoundException)
                        throwable = new ChannelException("Class " + className + " of received message not found", ex);  // Handle the exception
                    else if (ex instanceof ClassCastException)
                        throwable = new ChannelException("The class " + className + " is not assignable from MessageObject", ex); // Handle the exception
                    else throwable = ex;
                    channelResponse.getWaitingResponse().completeExceptionally(throwable);
                }
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                onResponseChannelSubscribe.accept(channel, subscribedChannels);
            }

            @Override
            public void onUnsubscribe(String channel, int subscribedChannels) {
                onResponseChannelUnsubscribe.accept(channel, subscribedChannels);
            }
        };
        this.responseReceiverThread = new Thread(() -> {
            sub(responseReceiverPubSub, receiverChannel);
        }, channel + "-response-receiver");
    }

    public void start() {
        responseReceiverThread.start();
    }

    public void shutdown() {
        responseReceiverPubSub.unsubscribe();
        responseReceiverThread.interrupt();
    }

    public <S extends MessageObject> CompletableFuture<RPCObjectChannelResponse<? extends MessageObject>> send(S messageObject) {
        return send(messageObject, 0, 0);
    }

    public <S extends MessageObject> CompletableFuture<RPCObjectChannelResponse<? extends MessageObject>> send(S messageObject, long timeout) {
        return send(messageObject, timeout, 0);
    }

    public <S extends MessageObject> CompletableFuture<RPCObjectChannelResponse<? extends MessageObject>> sendWithRemoteTimeout(S messageObject, long remoteTimeout) {
        return send(messageObject, 0, remoteTimeout);
    }

    public <S extends MessageObject> CompletableFuture<RPCObjectChannelResponse<? extends MessageObject>> send(S messageObject, long timeout, long remoteTimeout) {
        return FutureUtil.exceptionAsyncFuture(() -> {
            UUID correlationId = generateCorrelationId();
            RPCObjectChannelResponse<?> channelResponse = new RPCObjectChannelResponse<>();
            messagesWaitingResponse.put(correlationId, channelResponse);
            try {
                JsonObject object = new JsonObject();
                object.addProperty(DataKeys.CORRELATION_ID_KEY, correlationId.toString());
                object.addProperty(DataKeys.CLASS_NAME_KEY, messageObject.getClass().getName());
                object.add(DataKeys.MESSAGE_KEY, messageObject.serialize());
                object.addProperty(DataKeys.REMOTE_TIMEOUT_KEY, remoteTimeout);
                long status = pub(senderChannel, GsonUtil.GSON.toJson(object));
                if (status == 0) {
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
