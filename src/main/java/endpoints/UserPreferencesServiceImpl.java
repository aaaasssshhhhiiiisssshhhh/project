package endpoints;

import com.proto.common.Movie;
import com.proto.userpreferences.UserPreferencesRequest;
import com.proto.userpreferences.UserPreferencesResponse;
import com.proto.userpreferences.UserPreferencesServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.security.SecureRandom;

public class UserPreferencesServiceImpl extends
        UserPreferencesServiceGrpc.UserPreferencesServiceImplBase {
    @Override
    public StreamObserver<UserPreferencesRequest> getShortlistedMovies(StreamObserver<UserPreferencesResponse> responseObserver) {

        StreamObserver<UserPreferencesRequest> streamObserver =
                new StreamObserver<UserPreferencesRequest>() {


                    @Override
                    public void onNext(UserPreferencesRequest value) {
                        if (isEligible(value.getMovie())) {
                            responseObserver
                                    .onNext(UserPreferencesResponse
                                            .newBuilder()
                                            .setMovie(value.getMovie()).build());
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseObserver.onError(Status.INTERNAL
                                .withDescription("Internal server error")
                                .asRuntimeException());

                    }

                    @Override
                    public void onCompleted() {
                        responseObserver.onCompleted();
                    }
                };
        return streamObserver;
    }
    private boolean isEligible(Movie movie) {
        return (new SecureRandom().nextInt() % 4 != 0);
    }
}
