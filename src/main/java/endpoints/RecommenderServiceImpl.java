package endpoints;


import com.proto.common.Movie;
import com.proto.recommender.RecommenderRequest;
import com.proto.recommender.RecommenderResponse;
import com.proto.recommender.RecommenderServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class RecommenderServiceImpl extends RecommenderServiceGrpc.RecommenderServiceImplBase {
    @Override
    public StreamObserver<RecommenderRequest> getRecommendedMovie(StreamObserver<RecommenderResponse> responseObserver) {
        return new StreamObserver<RecommenderRequest>() {
            List<Movie> movies = new ArrayList<>();
            //            final List<Movie> movies = Arrays.asList(
//
//                    Movie.newBuilder().setTitle("Jadu").setDescription("alien form next planet").setGenre(Genre.DRAMA).setRating(8.1f).build(),
//
//                    Movie.newBuilder().setTitle("Jadu2").setDescription("alien from this planet").setGenre(Genre.THRILLER).setRating(2.5f).build(),
//
//                    Movie.newBuilder().setTitle("Jadu3").setDescription("alien from both planet").setGenre(Genre.COMEDY).setRating(2.7f).build(),
//
//                    Movie.newBuilder().setTitle("Jadu4").setDescription("alien ho re babuwa").setGenre(Genre.ACTION).setRating(5.5f).build(),
//
//                    Movie.newBuilder().setTitle("Jadu4").setDescription("alien form no planet").setGenre(Genre.ACTION).setRating(8.8f).build()
//            );
            @Override
            public void onNext(RecommenderRequest value) {
                System.out.println("movie: " + value.getMovie());
                try{
                    movies.add(value.getMovie());
                } catch (Exception e) {
                    System.out.println("error while adding movie!!!");
                    System.out.println(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("error");
                responseObserver.onError(Status.INTERNAL.withDescription("Internal server error").asRuntimeException());
            }

            @Override
            public void onCompleted() {
                System.out.println("i am herer");
                if (movies.isEmpty()) {
                    responseObserver.onError(Status.NOT_FOUND.withDescription("sorry found no movie").asRuntimeException());
                } else {
                    System.out.println("on completed");
                    responseObserver.onNext(RecommenderResponse.newBuilder().setMovie(findMovieRecommendation(movies)).build());
                }
            }
        };
    }

    private Movie findMovieRecommendation(List<Movie> movies) {
        System.out.println("find movie recommend" + movies);
        int random = new SecureRandom().nextInt(movies.size());
        Movie movie = movies.stream().skip(random).findAny().get();
        return movie;
    }
}
