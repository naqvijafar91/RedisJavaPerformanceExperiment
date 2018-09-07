import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisStringReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.nustaq.serialization.FSTConfiguration;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.*;

public class test {


    private int timeInGet =0;
    private int currentlyWorkingTasks=0;

    private FSTConfiguration conf;

    private VertxTest vertxTest;



    public static void main(String []arg){
        RedisClient redisClient = RedisClient.create("redis://@localhost:6379/0");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> syncCommands = connection.sync();

        List<String> keys=syncCommands.keys("*");
        System.out.println("Size:"+keys.size());

        test t=new test();
//        t.fetchSync(connection,keys);
//

//        t.fetchAllAsync(connection,keys);

//        t.fetchAllReactive(connection,keys);

        t.vertxTest.execute();
        try{
            Thread.sleep(150000);
                    connection.close();
        redisClient.shutdown();

        }catch (Exception e){

        }

    }

    public void fetchSync(StatefulRedisConnection<String,String> connection,List<String> keys) {
        RedisCommands<String, String> syncCommands = connection.sync();

        long current=System.currentTimeMillis();
        for (int i=0;i<keys.size();i++) {
            currentlyWorkingTasks++;
            long start=System.currentTimeMillis();
            String value=syncCommands.get(keys.get(i));
            long end=System.currentTimeMillis();
            timeInGet +=(end-start);
            vertxTest.processingCode(value);
            currentlyWorkingTasks--;
            if(currentlyWorkingTasks==0) {
                System.out.print("");
            }
        }

        if(currentlyWorkingTasks==0) {
            long now=System.currentTimeMillis();
            System.out.println("done Sync in :"+(now-current)+ " ms");
        }

        System.out.println("Total Time by GET library in Sync : "+ timeInGet);
    }

    public void fetchAllAsync(StatefulRedisConnection<String,String> connection,List<String> keys) {

        RedisAsyncCommands<String, String> commands = connection.async();

        final long current=System.currentTimeMillis();
        ExecutorService executor= Executors.newFixedThreadPool(500);


        for(int i=0;i<keys.size();i++) {

            RedisFuture<String> future = commands.get(keys.get(i));
            currentlyWorkingTasks++;
            future.thenAcceptAsync(new java.util.function.Consumer<String>() {
                @Override
                public void accept(String strings) {
                    vertxTest.processingCode(strings);
                    currentlyWorkingTasks--;

                        if(currentlyWorkingTasks==0) {
                            long now=System.currentTimeMillis();
                            System.out.println("done in :"+(now-current)+ " ms");
                            shutDownExecutor(executor);
                        }

                }
            },executor);
        }

    }

    private void shutDownExecutor(ExecutorService executor) {
        try {
            System.out.println("attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            System.err.println("tasks interrupted");
        }
        finally {
            if (!executor.isTerminated()) {
                System.err.println("cancel non-finished tasks");
            }
            executor.shutdownNow();
            System.out.println("shutdown finished");
        }

    }

    public void fetchAllReactive(StatefulRedisConnection<String,String> connection,List<String> keys){

        RedisStringReactiveCommands<String, String> commands = connection.reactive();
        ExecutorService executor= Executors.newFixedThreadPool(500);
        final long current=System.currentTimeMillis();
        currentlyWorkingTasks=0;
        for(int i=0;i<keys.size();i++) {

            currentlyWorkingTasks++;
            commands.get(keys.get(i)).subscribeOn(Schedulers.fromExecutor(executor)).subscribe(new java.util.function.Consumer<String>() {
                @Override
                public void accept(String strings) {

                    vertxTest.processingCode(strings);
                    currentlyWorkingTasks--;

                    if(currentlyWorkingTasks==0) {
                        long now=System.currentTimeMillis();
                        System.out.println("done Reactive in :"+(now-current)+ " ms");
                    }
                }
            });


        }

    }

    public test() {
         conf = FSTConfiguration.createDefaultConfiguration();
         vertxTest =new VertxTest();
    }
    private void Deserealize(String value) {

        Object obj=conf.asObject(value.getBytes());

    }
}
