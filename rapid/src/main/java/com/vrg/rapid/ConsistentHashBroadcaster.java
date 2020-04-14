package com.vrg.rapid;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.BroadcastingMessage;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.Metadata;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Broadcaster based on a ring of broadcasting nodes layed out via consistent hashing.
 *
 * Nodes in the cluster are segregated in two groups:
 * - broadcasting nodes that will relay messages to be broadcasted to all
 * - "broadastee" nodes that send the messages to be broadcasted to "their" broadcaster
 *
 * The goal is to minimize the amount of connections to other nodes that need to be maintained by a single node.
 * Broadcasting works as follows:
 * 1. A node that wants to broadcast a message sends it to it broadcaster
 * 2. The broadcaster forwards the message to all other broadcasters and to all other of its broadcastees
 * 3. Each broadcaster receiving a message forwards it to all of its broadcastees
 */
public class ConsistentHashBroadcaster implements IBroadcaster {

    static final int CONSISTENT_HASH_REPLICAS = 200;

    private final boolean isBroadcaster;
    private final IMessagingClient messagingClient;
    private final Endpoint myAddress;
    private Set<Endpoint> allRecipients = new HashSet<>();
    private Set<Endpoint> myRecipients = new HashSet<>();

    private ConsistentHash<Endpoint> broadcasterRing =
            new ConsistentHash<>(CONSISTENT_HASH_REPLICAS, Collections.emptyList());
    private Set<Endpoint> allBroadcasters = new HashSet<>();

    public ConsistentHashBroadcaster(final IMessagingClient messagingClient,
                                     final Endpoint myAddress,
                                     final Optional<Metadata> myMetadata) {
        this.messagingClient = messagingClient;
        this.myAddress = myAddress;
        this.isBroadcaster = myMetadata.isPresent() && isBroadcasterNode(myMetadata.get());
    }

    @Override
    @CanIgnoreReturnValue
    public List<ListenableFuture<RapidResponse>> broadcast(final RapidRequest rapidRequest,
                                                           final long configurationId) {
        final List<ListenableFuture<RapidResponse>> responses = new ArrayList<>();
        if (!broadcasterRing.isEmpty()) {
            final BroadcastingMessage.Builder broadcastingMessageBuilder = BroadcastingMessage.newBuilder()
                            .setMessage(rapidRequest)
                            .setConfigurationId(configurationId);

            if (isBroadcaster) {
                // directly send the message to other broadcasters for retransmission
                final RapidRequest request = Utils.toRapidRequest(
                        broadcastingMessageBuilder.setShouldDeliver(true).build()
                );
                sendToBroadcasters(request);
                sendToOwnRecipients(rapidRequest);
            } else {
                // send it to our broadcaster who will take care of retransmission
                final RapidRequest request = Utils.toRapidRequest(
                        broadcastingMessageBuilder.setShouldDeliver(false).build()
                );
                final Endpoint broadcaster = broadcasterRing.get(myAddress);
                final ListenableFuture<RapidResponse> response = messagingClient.sendMessage(broadcaster, request);
                responses.add(response);
            }
        } else {
            // fall back to direct broadcasting
            // this happens e.g. when the seed node joins
            allRecipients.forEach(node -> {
                responses.add(messagingClient.sendMessageBestEffort(node, rapidRequest));
            });

        }
        return responses;
    }

    @Override
    public void setInitialMembership(final List<Endpoint> recipients, final Map<Endpoint, Metadata> metadataMap) {
        allRecipients.addAll(recipients);
        metadataMap.forEach((node, metadata) -> {
            if (isBroadcasterNode(metadata)) {
                broadcasterRing.add(node);
                allBroadcasters.add(node);
            }
        });

        if (isBroadcaster) {
            recipients
                    .stream()
                    .filter(node -> myAddress.equals(broadcasterRing.get(node)))
                    .forEach(node -> myRecipients.add(node));
        }
    }

    @Override
    public void onNodeAdded(final Endpoint node, final Optional<Metadata> metadata) {
        allRecipients.add(node);
        final boolean addedNodeIsBroadcaster = metadata.isPresent() && isBroadcasterNode(metadata.get());
        if (!allBroadcasters.contains(node) && addedNodeIsBroadcaster) {
            broadcasterRing.add(node);
            allBroadcasters.add(node);
        }
        if (isBroadcaster) {
            final Endpoint responsibleBroadcaster = broadcasterRing.get(node);
            if (myAddress.equals(responsibleBroadcaster)) {
                myRecipients.add(node);
            }
        }
    }

    @Override
    public void onNodeRemoved(final Endpoint node) {
        allRecipients.remove(node);
        broadcasterRing.remove(node);
        allBroadcasters.remove(node);
        myRecipients.remove(node);
    }

    public ListenableFuture<RapidResponse> handleBroadcastingMessage(final BroadcastingMessage msg) {
        final SettableFuture<RapidResponse> future = SettableFuture.create();
        if (msg.getShouldDeliver()) {
            // this message already went through the ring, deliver to our recipients
            sendToOwnRecipients(msg.getMessage());
        } else {
            // pass it on to all other broadcasters, and deliver to our recipients
            sendToBroadcasters(Utils.toRapidRequest(msg.toBuilder().setShouldDeliver(true).build()));
            sendToOwnRecipients(msg.getMessage());
        }
        future.set(null);
        return future;
    }

    private boolean isBroadcasterNode(final Metadata nodeMetadata) {
        return nodeMetadata.getMetadataCount() > 0
                && nodeMetadata.getMetadataMap().containsKey(Cluster.BROADCASTER_METADATA_KEY);
    }

    private List<ListenableFuture> sendToOwnRecipients(final RapidRequest msg) {
        final List<ListenableFuture> responses = new ArrayList<>();
        myRecipients.forEach(node -> {
            responses.add(messagingClient.sendMessageBestEffort(node, msg));
        });
        return responses;
    }

    private List<ListenableFuture> sendToBroadcasters(final RapidRequest request) {
        final List<ListenableFuture> responses = new ArrayList<>();
        allBroadcasters.stream().filter(node -> !node.equals(myAddress)).forEach(node -> {
            final ListenableFuture<RapidResponse> response = messagingClient.sendMessage(node, request);
            responses.add(response);
        });
        return responses;
    }


}