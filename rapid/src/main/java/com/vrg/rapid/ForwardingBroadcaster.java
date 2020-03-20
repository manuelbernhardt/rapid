package com.vrg.rapid;

import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.BroadcastingMessage;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Broadcaster that forwards the message to a node that will in turn broadcast it
 */
public class ForwardingBroadcaster implements IBroadcaster {
    private final IMessagingClient messagingClient;
    private final List<Endpoint> broadcasters;

    private List<Endpoint> recipients = Collections.emptyList();

    public ForwardingBroadcaster(final IMessagingClient messagingClient, final List<Endpoint> broadcasters) {
        this.messagingClient = messagingClient;
        this.broadcasters = broadcasters;
    }

    @Override
    public List<ListenableFuture<RapidResponse>> broadcast(final RapidRequest rapidRequest) {
        final RapidRequest broadcastingRequest = RapidRequest.newBuilder().setBroadcastingMessage(
                BroadcastingMessage.newBuilder().setMessage(rapidRequest).addAllRecipients(recipients)
        ).build();
        final List<ListenableFuture<RapidResponse>> responses = new ArrayList<>();
        final int index = ThreadLocalRandom.current().nextInt(broadcasters.size());
        responses.add(messagingClient.sendMessage(broadcasters.get(index), broadcastingRequest));
        return responses;
    }

    @Override
    public void setMembership(final List<Endpoint> recipients) {
        this.recipients = recipients;
    }
}
