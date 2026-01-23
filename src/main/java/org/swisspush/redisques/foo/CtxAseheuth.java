package org.swisspush.redisques.foo;

import io.vertx.redis.client.RedisAPI;
import org.swisspush.redisques.queue.QueueProcessingState;
import org.swisspush.redisques.scheduling.PeriodicSkipScheduler;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;


public class CtxAseheuth {

    RedisQuesGroupedExecutor executor;
    RedisAPI redis;
    PeriodicSkipScheduler periodicSkipScheduler;
    Supplier<Map<String, QueueProcessingState>> getQueues;
    Function<String, String> getMyConsumerNameByQueueName;
    Supplier<String> getMyVerticleId;
    long periodMs;

    CtxAseheuth() { }

}
