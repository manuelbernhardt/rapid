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
import java.util.Set;
import java.util.HashSet;

/**
 * Broadcaster that forwards the message to a node that will in turn broadcast it
 */
public class ForwardingBroadcaster implements IBroadcaster {

    static final int CONSISTENT_HASH_REPLICAS = 200;

    private final IMessagingClient messagingClient;
    private final Set<Endpoint> broadcasters;
    private final Endpoint myAddress;

    private List<Endpoint> recipients = Collections.emptyList();

    private ConsistentHash<Endpoint> availableBroadcasters = new ConsistentHash<>(0, Collections.emptyList());

    private final IBroadcaster fallback ;

    public ForwardingBroadcaster(final IMessagingClient messagingClient, final Set<Endpoint> broadcasters,
                                 final Endpoint myAddress) {
        this.messagingClient = messagingClient;
        this.broadcasters = broadcasters;
        this.myAddress = myAddress;
        this.fallback = new UnicastToAllBroadcaster(messagingClient);
    }

    @Override
    public List<ListenableFuture<RapidResponse>> broadcast(final RapidRequest rapidRequest) {
        if (!availableBroadcasters.isEmpty()) {
            final RapidRequest broadcastingRequest = RapidRequest.newBuilder().setBroadcastingMessage(
                    BroadcastingMessage.newBuilder().setMessage(rapidRequest).addAllRecipients(recipients)
            ).build();
            final List<ListenableFuture<RapidResponse>> responses = new ArrayList<>();
            // use consistent hashing to minimize the amount of connections this node has to maintain with broadcasters
            final ListenableFuture<RapidResponse> broadcastResponse =
                    messagingClient.sendMessage(availableBroadcasters.get(myAddress), broadcastingRequest);
            responses.add(broadcastResponse);
            return responses;
        } else {
            return fallback.broadcast(rapidRequest);
        }
    }

    @Override
    public void setMembership(final List<Endpoint> recipients) {
        this.recipients = recipients;
        // only retain broadcasters that are part of the membership set
        final Set<Endpoint> potentialBroadcasters = new HashSet<>(recipients);
        potentialBroadcasters.retainAll(broadcasters);
        this.availableBroadcasters = new ConsistentHash<>(CONSISTENT_HASH_REPLICAS, potentialBroadcasters);
    }

}