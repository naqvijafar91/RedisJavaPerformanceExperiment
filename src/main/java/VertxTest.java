import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

public class VertxTest {

    private JsonArray keys;
    private RedisClient redis;
    private int runningTasks=0;
    private Vertx vertx;

    public VertxTest() {
        vertx = Vertx.vertx();
        RedisOptions config = new RedisOptions()
                .setHost("127.0.0.1");

        redis = RedisClient.create(vertx, config);
    }

    public void execute() {
        redis.keys("*", new Handler<AsyncResult<JsonArray>>() {
            @Override
            public void handle(AsyncResult<JsonArray> event) {

//                System.out.println(event.result().encodePrettily());
                System.out.println("Total Number of Keys : "+event.result().size());

                keys=event.result();

                fetchAllKeys();

            }
        });
    }

    private void fetchAllKeys() {

        runningTasks=0;
        final long current=System.currentTimeMillis();
        for(int i=0;i<this.keys.size();i++) {

            runningTasks++;
            this.redis.get(this.keys.getString(i), new Handler<AsyncResult<String>>() {
                @Override
                public void handle(AsyncResult<String> event) {

                    vertx.executeBlocking(future -> {
                        // Call some blocking API that takes a significant amount of time to return
                        processingCode(event.result());
                        future.complete();
                    }, res -> {
                        runningTasks--;
                        if(runningTasks==0) {
                            long now=System.currentTimeMillis();
                            System.out.println("done Vertx in :"+(now-current)+ " ms");
                        }
                    });



                }
            });

        }

    }

    public void processingCode(String value){
        for(int j=0;j<1500;j++) {
            if(value.isEmpty() || j<1 || value.charAt(0)!='a') {
                System.out.print("");
            }
        }

    }
}
