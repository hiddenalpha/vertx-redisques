package org.swisspush.redisques.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

import static java.lang.System.currentTimeMillis;
import static org.slf4j.LoggerFactory.getLogger;

import static java.lang.Thread.currentThread;

/**
 * Class StatisticsCollector helps collecting statistics information about queue handling and
 * failures.
 * <p>
 * Due to the fact that there is a Redisques responsible for one queue instance at the same time
 * in a cluster, we could assume that the values are good enough when cached locally for queue
 * processing by itself. If the responsible Redisques Instance changes, it will build up the
 * statistics straight away by itself anyway.
 * <p>
 * But in the case of the statistics read operation, we must get the statistics of all queues
 * existing, not only the ones which are treated by the redisques instance (there might be multiple
 * involved). Therefore, we must write the statistics values as well to redis and retrieve them from
 * there once needed. Note that the statistics is written asynch and only in the case of queue
 * failures, therefore it shouldn't happen too often.
 */
public class QueueStatisticsCollector {

    private static final Logger log = getLogger(QueueStatisticsCollector.class);
    private final EventBus evBus;
    private final Lock mutx = new ReentrantLock();
    private Thread mutxOwner;
    private final Map<String, Queue> queues = new HashMap<>();
    private final Set<Queue> hasLocalChanges = new HashSet<>();

    public QueueStatisticsCollector(
        RedisProvider redisProvider, String queuesPrefix, Vertx vertx, RedisQuesExceptionFactory exceptionFactory,
        Semaphore redisMonitoringReqQuota, int queueSpeedIntervalSec
    ) {
        long publishEveryMs = 1000;
        String evBusAddr = QueueStatisticsCollector.class.getCanonicalName();
        evBus = vertx.eventBus();
        evBus.consumer(evBusAddr, this::onUpdateFromClusterPeer);
        vertx.setTimer(publishEveryMs, this::publishLocalChanges);
    }

    private void onUpdateFromClusterPeer(Message<Object> evBusMsg) {
        Object body = evBusMsg.body();
        assert false : "TODO";
    }

    private void publishLocalChanges(long nonsense) {
        Set<Queue> localCopy;
        mutx.lock();
        try {
            localCopy = hasLocalChanges;
        } finally {
            mutx.unlock();
        }
        evBus.publish(, );
    }

    public void resetQueueFailureStatistics(String queueName, BiConsumer<Throwable, Void> onDone) {
        log.trace("resetQueueFailureStatistics(\"{}\")", queueName);
    }

    public void queueMessageSuccess(String queueName, BiConsumer<Throwable, Void> onDone) {
        long now = currentTimeMillis();
        mutx.lock();
        try {
            assert mutxOwner == null;
            mutxOwner = currentThread();
            Queue queue = getQueue(queueName);
            assert queue.name.equals(queueName) : queue.name +", "+ queueName;
            queue.newestSuccessEpchMs = now;
            hasLocalChanges.add(queue);
        } finally {
            mutxOwner = null;
            mutx.unlock();
        }
    }


    /**
     * @return failureCount
     */
    public long queueMessageFailed(String queueName) {
        long now = currentTimeMillis();
        mutx.lock();
        try {
            assert mutxOwner == null;
            mutxOwner = currentThread();
            Queue queue = getQueue(queueName);
            queue.newestFailureEpchMs = now;
            hasLocalChanges.add(queue);
        } finally {
            mutxOwner = null;
            mutx.unlock();
        }
        return 1;
    }

    public void setQueueSlowDownTime(String queueName, int retryDelaySec) {
        mutx.lock();
        try {
            assert mutxOwner == null;
            mutxOwner = currentThread();
            Queue queue = getQueue(queueName);
            assert queue.name.equals(queueName);
            queue.slowdownTimeSec = retryDelaySec;
            hasLocalChanges.add(queue);
        } finally {
            mutxOwner = null;
            mutx.unlock();
        }
    }

    public void setQueueBackPressureTime(String queueName, long delayReplyMs) {
        mutx.lock();
        assert mutxOwner == null;
        mutxOwner = currentThread();
        try {
            Queue queue = getQueue(queueName);
            queue.backpressureDelayMs = delayReplyMs;
            hasLocalChanges.add(queue);
        } finally {
            mutxOwner = null;
            mutx.unlock();
        }
    }

    // TODO get rid of ubly 'JsonObject' in return
    public Future<JsonObject> getQueueStatistics(List<String> queueNames) {
        log.trace("TODO getQueueStatistics({})", queueNames);
        return Promise.<JsonObject>promise().future(/*TODO*/);
    }

    public void getQueuesSpeed(Message<JsonObject> ev, List<String> queueNames) {
        throw new UnsupportedOperationException/*TODO*/("not impl yet");
    }

    public void resetQueueStatistics(JsonArray queues, BiConsumer<Throwable, Void> todoQ93258hu38ErrorHandling) {
        throw new UnsupportedOperationException/*TODO*/("not impl yet");
    }

    private Queue getQueue(String queueName) {
        assert mutxOwner == currentThread();
        Queue queue = queues.get(queueName);
        if(queue == null){
            queue = new Queue();
            queue.name = queueName;
            queues.put(queue.name, queue);
        }
        return queue;
    }

    private static class Queue {
        private String name;
        private long newestSuccessEpchMs;
        private long newestFailureEpchMs;
        private int slowdownTimeSec;
        private long backpressureDelayMs;
    }

}
