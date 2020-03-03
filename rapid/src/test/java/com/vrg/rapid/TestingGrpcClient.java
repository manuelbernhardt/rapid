package com.vrg.rapid;

import com.vrg.rapid.pb.Endpoint;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.messaging.impl.GrpcClient;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;

import java.util.List;

/**
 * GrpcClient with interceptors used for testing
 */
class TestingGrpcClient extends GrpcClient {
    private final List<ClientInterceptors.RequestDelayer> requestInterceptors;
    private final List<ClientInterceptors.ResponseDelayer> responseInterceptors;

    TestingGrpcClient(final Endpoint address, final ISettings settings,
                      final List<ClientInterceptors.RequestDelayer> requestInterceptors,
                      final List<ClientInterceptors.ResponseDelayer> responseInterceptors) {
        super(address, settings);
        this.requestInterceptors = requestInterceptors;
        this.responseInterceptors = responseInterceptors;
    }

    /**
     * From IMessagingClient
     */
    @Override
    public ListenableFuture<RapidResponse> sendMessage(final Endpoint remote, final RapidRequest msg) {
        for (final ClientInterceptors.RequestDelayer interceptor: requestInterceptors) {
            if (!interceptor.filter(msg)) {
                return Futures.immediateFuture(RapidResponse.getDefaultInstance());
            }
        }

        final ListenableFuture<RapidResponse> responseFuture = super.sendMessage(remote, msg);
        return Futures.transform(responseFuture, response -> {
            for (final ClientInterceptors.ResponseDelayer interceptor : responseInterceptors) {
                if (!interceptor.filter(response)) {
                    return response;
                } else {
                    return RapidResponse.getDefaultInstance();
                }
            }
            return response;
        });
    }
}
