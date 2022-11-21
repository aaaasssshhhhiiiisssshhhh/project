package endpoints;

import com.proto.moviecontroller.MovieControllerServiceGrpc;
import com.proto.moviecontroller.MovieRequest;
import com.proto.moviecontroller.MovieResponse;
import com.proto.moviestore.MovieStoreRequest;
import com.proto.moviestore.MovieStoreServiceGrpc;
import com.proto.recommender.RecommenderRequest;
import com.proto.recommender.RecommenderResponse;
import com.proto.recommender.RecommenderServiceGrpc;
import com.proto.userpreferences.UserPreferencesRequest;
import com.proto.userpreferences.UserPreferencesResponse;
import com.proto.userpreferences.UserPreferencesServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MovieControllerServiceImpl extends
        MovieControllerServiceGrpc.MovieControllerServiceImplBase {
    public static final int MOVIES_SERVICE_PORT = 50052;
    public static final int USER_PREFERENCES_SERVICE_PORT = 50053;
    public static final int RECOMMENDER_SERVICE_PORT = 50054;

    @Override
    public void getMovie(MovieRequest request, StreamObserver<MovieResponse> responseObserver) {

        String userId = request.getUserid();
        MovieStoreServiceGrpc.MovieStoreServiceBlockingStub
                movieStoreClient =
                MovieStoreServiceGrpc
                        .newBlockingStub(getChannel(MOVIES_SERVICE_PORT));
        UserPreferencesServiceGrpc.UserPreferencesServiceStub
                userPreferencesClient = UserPreferencesServiceGrpc
                .newStub(getChannel(USER_PREFERENCES_SERVICE_PORT));
        RecommenderServiceGrpc.RecommenderServiceStub
                recommenderClient =
                RecommenderServiceGrpc
                        .newStub(getChannel(RECOMMENDER_SERVICE_PORT));

        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<RecommenderRequest>
                recommenderRequestObserver =
                recommenderClient
                        .getRecommendedMovie(new
                                                     StreamObserver<RecommenderResponse>() {
                                                         public void onNext(RecommenderResponse value) {
                                                             responseObserver.onNext(MovieResponse
                                                                     .newBuilder()
                                                                     .setMovie(value.getMovie()).build());
                                                             System.out.println("Recommended movie " +
                                                                     value.getMovie());
                                                         }
                                                         public void onError(Throwable t) {
                                                             responseObserver.onError(t);
                                                             latch.countDown();
                                                         }
                                                         public void onCompleted() {
                                                             responseObserver.onCompleted();
                                                             latch.countDown();
                                                         }
                                                     });
        StreamObserver<UserPreferencesRequest>
                streamObserver =
                userPreferencesClient
                        .getShortlistedMovies(new
                                                      StreamObserver<UserPreferencesResponse>() {
                                                          public void onNext(UserPreferencesResponse value){
                                                              recommenderRequestObserver
                                                                      .onNext(RecommenderRequest.newBuilder()
                                                                              .setUserid(userId)
                                                                              .setMovie(value.getMovie()).build());
                                                          }

                                                          public void onError(Throwable t) {
                                                          }
                                                          @Override
                                                          public void onCompleted() {
                                                              recommenderRequestObserver.onCompleted();
                                                          }
                                                      });
        movieStoreClient.getMovies(MovieStoreRequest.newBuilder()
                        .setGenre(request.getGenre()).build())
                .forEachRemaining(response -> {
                    streamObserver
                            .onNext(UserPreferencesRequest.newBuilder()
                                    .setUserid(userId).setMovie(response.getMovie())
                                    .build());
                });
        streamObserver.onCompleted();
        try {
            latch.await(3L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private Channel getChannel(int port) {
        return ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
    }
}