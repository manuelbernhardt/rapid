package com.vrg.rapid;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.ByteString;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.BroadcastingMessage;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.FastRoundPhase2bMessage;
import com.vrg.rapid.pb.Metadata;
import com.vrg.rapid.pb.Proposal;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private static final Logger LOG = LoggerFactory.getLogger(ConsistentHashBroadcaster.class);

    private final boolean isBroadcaster;
    private final IMessagingClient messagingClient;
    private final MembershipView membershipView;
    private final Endpoint myAddress;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    @GuardedBy("rwLock") private Set<Endpoint> allRecipients = new HashSet<>();
    @GuardedBy("rwLock") private Set<Endpoint> myRecipients = new HashSet<>();

    private ConsistentHash<Endpoint> broadcasterRing =
            new ConsistentHash<>(CONSISTENT_HASH_REPLICAS, Collections.emptyList());
    private Set<Endpoint> allBroadcasters = new HashSet<>();

    // batching of votes
    private long lastRecipientEnqueueTimestamp = -1;    // Timestamp
    private long lastBroadcasterEnqueueTimestamp = -1;    // Timestamp
    @GuardedBy("broadcastersBatchQueueLock")
    private final LinkedBlockingQueue<FastRoundPhase2bMessage> broadcastersSendQueue = new LinkedBlockingQueue<>();
    @GuardedBy("recipientsBatchQueueLock")
    private final LinkedBlockingQueue<FastRoundPhase2bMessage> recipientsSendQueue = new LinkedBlockingQueue<>();
    private final Lock broadcastersBatchQueueLock = new ReentrantLock();
    private final Lock recipientsBatchQueueLock = new ReentrantLock();
    private final ScheduledExecutorService backgroundTasksExecutor;
    private ScheduledFuture<?> consensusBatchingJob;
    private final int consensusBatchingWindowInMs;

    public ConsistentHashBroadcaster(final IMessagingClient messagingClient,
                                     final MembershipView membershipView,
                                     final Endpoint myAddress,
                                     final Optional<Metadata> myMetadata,
                                     final SharedResources sharedResources,
                                     final int consensusBatchingWindowInMs) {
        this.messagingClient = messagingClient;
        this.membershipView = membershipView;
        this.myAddress = myAddress;
        this.isBroadcaster = myMetadata.isPresent() && isBroadcasterNode(myMetadata.get());
        this.consensusBatchingWindowInMs = consensusBatchingWindowInMs;

        // Schedule fast paxos job
        this.backgroundTasksExecutor = sharedResources.getScheduledTasksExecutor();
        if (isBroadcaster) {
            // TODO cancel when shut down
            consensusBatchingJob = this.backgroundTasksExecutor.scheduleAtFixedRate(
                    new ConsistentHashBroadcaster.FastPaxosBatcher(),
                    0, consensusBatchingWindowInMs, TimeUnit.MILLISECONDS);
        }

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
                sendToBroadcastersAndRecipients(broadcastingMessageBuilder.setShouldDeliver(true).build());
            } else {
                // send it to our broadcaster who will take care of retransmission
                final RapidRequest request = Utils.toRapidRequest(
                        broadcastingMessageBuilder.setShouldDeliver(false).build()
                );
                final Endpoint broadcaster = broadcasterRing.get(myAddress);
                final ListenableFuture<RapidResponse> response = messagingClient.sendMessage(broadcaster, request);
                Futures.addCallback(response, new ErrorReportingCallback(request.getContentCase(), broadcaster));
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
        rwLock.readLock().lock();
        try {
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
                assert myRecipients.contains(myAddress);
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void onNodeAdded(final Endpoint node, final Optional<Metadata> metadata) {
        rwLock.readLock().lock();
        try {
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
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void onNodeRemoved(final Endpoint node) {
        rwLock.readLock().lock();
        try {
            allRecipients.remove(node);
            broadcasterRing.remove(node);
            allBroadcasters.remove(node);
            myRecipients.remove(node);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public ListenableFuture<RapidResponse> handleBroadcastingMessage(final BroadcastingMessage msg) {
        final SettableFuture<RapidResponse> future = SettableFuture.create();
        if (msg.getShouldDeliver()) {
            // this message already went through the ring, deliver to our recipients
            sendToOwnRecipients(msg.getMessage(), true);
        } else {
            // pass it on to all other broadcasters, and deliver to our recipients
            sendToBroadcastersAndRecipients(msg.toBuilder().setShouldDeliver(true).build());
        }
        future.set(null);
        return future;
    }

    private boolean isBroadcasterNode(final Metadata nodeMetadata) {
        return nodeMetadata.getMetadataCount() > 0
                && nodeMetadata.getMetadataMap().containsKey(Cluster.BROADCASTER_METADATA_KEY);
    }

    private void sendToOwnRecipients(final RapidRequest msg, final boolean shouldBatch) {
        if (shouldBatch && msg.getContentCase() == RapidRequest.ContentCase.FASTROUNDPHASE2BMESSAGE) {
            enqueueRecipientMessage(msg.getFastRoundPhase2BMessage());
        } else {
            try {
                rwLock.readLock().lock();
                // send to myself first
                Futures.addCallback(messagingClient.sendMessage(myAddress, msg),
                        new ErrorReportingCallback(msg.getContentCase(), myAddress));

                // then to the rest
                myRecipients.forEach(node -> {
                    if (!node.equals(myAddress)) {
                        Futures.addCallback(messagingClient.sendMessage(node, msg),
                                new ErrorReportingCallback(msg.getContentCase(), myAddress));
                    }
                });
            } finally {
                rwLock.readLock().unlock();
            }
        }
    }

    @CanIgnoreReturnValue
    private ListenableFuture<?> sendToBroadcastersAndRecipients(final BroadcastingMessage broadcastingMessage) {
        // first send to the broadcasters and only then to the recipients
        final ListenableFuture<List<RapidResponse>> broadcasterResponses =
                Futures.successfulAsList(sendToBroadcasters(broadcastingMessage, true));

        return Futures.transform(broadcasterResponses, rapidResponses -> {
            if (!broadcastingMessage.getMessage().getContentCase()
			.equals(RapidRequest.ContentCase.FASTROUNDPHASE2BMESSAGE)
                    && rapidResponses.size() != allBroadcasters.size() - 1) {
                LOG.warn("Could not deliver request of type {} to all broadcasters!",
                        broadcastingMessage.getMessage().getContentCase().name());
            }
            sendToOwnRecipients(broadcastingMessage.getMessage(), true);
            return rapidResponses;
        });
    }

    private List<ListenableFuture<RapidResponse>> sendToBroadcasters(final BroadcastingMessage message,
                                                                     final boolean shouldBatch) {
        final List<ListenableFuture<RapidResponse>> responses = new ArrayList<>();
        if (shouldBatch &&
                message.getMessage().getContentCase().equals(RapidRequest.ContentCase.FASTROUNDPHASE2BMESSAGE)) {
            enqueueBroadcasterMessage(message.getMessage().getFastRoundPhase2BMessage());
        } else {
            rwLock.readLock().lock();
            try {
                allBroadcasters.stream().filter(node -> !node.equals(myAddress)).forEach(node -> {
                    final ListenableFuture<RapidResponse> response =
                            messagingClient.sendMessage(node, Utils.toRapidRequest(message));
                    Futures.addCallback(response,
                            new ErrorReportingCallback(message.getMessage().getContentCase(), node));
                    responses.add(response);
                });
            } finally {
                rwLock.readLock().unlock();
            }
        }
        return responses;
    }

    private void enqueueRecipientMessage(final FastRoundPhase2bMessage msg) {
        recipientsBatchQueueLock.lock();
        try {
            lastRecipientEnqueueTimestamp = System.currentTimeMillis();
            recipientsSendQueue.add(msg);
        }
        finally {
            recipientsBatchQueueLock.unlock();
        }
    }

    private void enqueueBroadcasterMessage(final FastRoundPhase2bMessage msg) {
        broadcastersBatchQueueLock.lock();
        try {
            lastBroadcasterEnqueueTimestamp = System.currentTimeMillis();
            broadcastersSendQueue.add(msg);
        }
        finally {
            broadcastersBatchQueueLock.unlock();
        }
    }


    private class FastPaxosBatcher implements Runnable {

        @Override
        public void run() {
            final ArrayList<FastRoundPhase2bMessage> broadcasterMessages =
                    new ArrayList<>(broadcastersSendQueue.size());
            broadcastersBatchQueueLock.lock();
            try {
                // Wait one CONSENSUS_BATCHING_WINDOW_IN_MS since last add before sending out
                if (!broadcastersSendQueue.isEmpty() && lastBroadcasterEnqueueTimestamp > 0
                        && (System.currentTimeMillis() - lastBroadcasterEnqueueTimestamp) >
                        consensusBatchingWindowInMs) {
                    final int numDrained = broadcastersSendQueue.drainTo(broadcasterMessages);
                    assert numDrained > 0;
                }
            } finally {
                broadcastersBatchQueueLock.unlock();
            }
            if (!broadcasterMessages.isEmpty()) {
                final FastRoundPhase2bMessage batched = compressConsensusMessages(broadcasterMessages);
                final BroadcastingMessage message = BroadcastingMessage.newBuilder()
                        .setMessage(Utils.toRapidRequest(batched))
                        .setShouldDeliver(true)
                        .setConfigurationId(membershipView.getCurrentConfigurationId())
                        .build();
                sendToBroadcasters(message, false);
            }

            final ArrayList<FastRoundPhase2bMessage> recipientMessages = new ArrayList<>(recipientsSendQueue.size());
            recipientsBatchQueueLock.lock();
            try {
                // Wait one CONSENSUS_BATCHING_WINDOW_IN_MS since last add before sending out
                if (!recipientsSendQueue.isEmpty() && lastRecipientEnqueueTimestamp > 0
                        && (System.currentTimeMillis() - lastRecipientEnqueueTimestamp) > consensusBatchingWindowInMs) {
                    final int numDrained = recipientsSendQueue.drainTo(recipientMessages);
                    assert numDrained > 0;
                }
            } finally {
                recipientsBatchQueueLock.unlock();
            }

            if (!recipientMessages.isEmpty()) {
                final FastRoundPhase2bMessage batched = compressConsensusMessages(recipientMessages);
                sendToOwnRecipients(Utils.toRapidRequest(batched), false);
            }
        }

        private FastRoundPhase2bMessage compressConsensusMessages(final List<FastRoundPhase2bMessage> messages) {
            // compress all the messages into one
            final Map<List<Endpoint>, BitSet> allProposals = new HashMap<>();
            for (final FastRoundPhase2bMessage consensusMessage : messages) {
                for (final Proposal proposal : consensusMessage.getProposalsList()) {
                    final BitSet allVotes = allProposals.computeIfAbsent(proposal.getEndpointsList(),
                            endpoints -> BitSet.valueOf(proposal.getVotes().toByteArray()));
                    allVotes.or(BitSet.valueOf(proposal.getVotes().toByteArray()));
                    allProposals.put(proposal.getEndpointsList(), allVotes);
                }
            }
            final List<Proposal> allProposalMessages = new ArrayList<>();
            allProposals.entrySet().forEach(entry -> {
                final Proposal proposal = Proposal.newBuilder()
                        .addAllEndpoints(entry.getKey())
                        .setVotes(ByteString.copyFrom(entry.getValue().toByteArray()))
                        .build();
                allProposalMessages.add(proposal);
            });
            final FastRoundPhase2bMessage batched = FastRoundPhase2bMessage.newBuilder()
                    .setConfigurationId(membershipView.getCurrentConfigurationId())
                    .addAllProposals(allProposalMessages)
                    .build();
            return batched;
        }


    }

    private static class ErrorReportingCallback implements FutureCallback<RapidResponse> {
        private final RapidRequest.ContentCase requestType;
        private final Endpoint recipient;

        public ErrorReportingCallback(final RapidRequest.ContentCase requestType, final Endpoint recipient) {
            this.requestType = requestType;
            this.recipient = recipient;
        }

        private static final Logger LOG = LoggerFactory.getLogger(ConsistentHashBroadcaster.class);

        @Override
        public void onSuccess(@Nullable final RapidResponse rapidResponse) {
        }

        @Override
        public void onFailure(final Throwable throwable) {
            LOG.warn("Failed to broadcast request of type {} to {}:{}",
                    requestType.name(), recipient.getHostname().toStringUtf8(), recipient.getPort());
        }
    }

}
