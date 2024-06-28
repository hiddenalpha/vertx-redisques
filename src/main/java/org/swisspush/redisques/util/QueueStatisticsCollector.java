package org.swisspush.redisques.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonBooleanFormatVisitor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.exception.RedisQuesExceptionFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static org.slf4j.LoggerFactory.getLogger;

import static io.vertx.core.Future.succeededFuture;
import static java.lang.Thread.currentThread;
import static org.swisspush.redisques.util.RedisquesAPI.MONITOR_QUEUE_NAME;
import static org.swisspush.redisques.util.RedisquesAPI.QUEUES;
import static org.swisspush.redisques.util.RedisquesAPI.STATUS;

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

    private static final String queueUpdatesAddr = "redisques." + QueueStatisticsCollector.class.getSimpleName() + ".jAQCAO0YAgDrSQIA";
    private final long publishEveryMs = 10_000; /*TODO ctor-inject*/
    private static final Logger log = getLogger(QueueStatisticsCollector.class);
    private final Vertx vertx;
    private final EventBus evBus;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Lock mutx = new ReentrantLock();
    private Thread mutxOwner;
    private final Map<String, QueueInternal> queues = new HashMap<>();
    private Set<QueueInternal> hasLocalChanges = new HashSet<>();

    public QueueStatisticsCollector(
            RedisProvider redisProvider, String queuesPrefix, Vertx vertx, RedisQuesExceptionFactory exceptionFactory,
            Semaphore redisMonitoringReqQuota, int queueSpeedIntervalSec
    ) {
        this.vertx = vertx;
        this.evBus = vertx.eventBus();
        this.evBus.consumer(queueUpdatesAddr, this::onUpdateFromCluster);
        this.vertx.setTimer(publishEveryMs, this::publishLocalChanges);
    }

    private void publishLocalChanges(long nonsense) {
        Set<QueueInternal> copyToPublish;
        // TODO make 'hasLocalchanges.size()' thread-save.
        Set<QueueInternal> theNewInstance = new HashSet<>(max(16, hasLocalChanges.size() / 2));
        mutx.lock();
        try {
            copyToPublish = hasLocalChanges;
            hasLocalChanges = theNewInstance;
        } finally {
            mutx.unlock();
        }
        var buf = new ByteArrayOutputStream(64);
        var writer = new OutputStreamWriter(buf);
        encode(copyToPublish, writer);
        try {
            writer.close();
        } catch (IOException e) {
            throw new UncheckedIOException("TODO", e);
        }
        evBus.publish(queueUpdatesAddr, buf.toByteArray());
        vertx.setTimer(publishEveryMs, this::publishLocalChanges);
    }

    private void onUpdateFromCluster(Message<Object> evBusMsg) {
        Set<QueueInternal> newQueues = decode((byte[]) evBusMsg.body());
        mutx.lock();
        try {
            /*TODO any way to move this loop outside lock?*/
            for (QueueInternal newQueue : newQueues) {
                queues.put(newQueue.name, newQueue);
            }
        } finally {
            mutx.unlock();
        }
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
            QueueInternal queue = getQueue(queueName);
            assert queue.name.equals(queueName) : queue.name + ", " + queueName;
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
            QueueInternal queue = getQueue(queueName);
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
            QueueInternal queue = getQueue(queueName);
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
            QueueInternal queue = getQueue(queueName);
            queue.backpressureDelayMs = delayReplyMs;
            hasLocalChanges.add(queue);
        } finally {
            mutxOwner = null;
            mutx.unlock();
        }
    }

    /**
     * @deprecated use {link #getQueueStatistics2}. WARN: This API bleeds
     * weakly-typed stuff! The compiler will FAIL to tell you about bugs.
     */
    @Deprecated
    public Future<JsonObject> getQueueStatistics(List<String> queueNames) {
        return getQueueStatistics2(queueNames).compose(queues -> vertx.executeBlocking(() -> {
            JsonArray superfluousJsonArray = new JsonArray();
            for (Queue queue : queues) {
                JsonObject queueJson = new JsonObject();
                queueJson.put(MONITOR_QUEUE_NAME, queue.name);
                /*queueJson.put(TODO, queue.TODO);*/
                superfluousJsonArray.add(queueJson);
            }
            JsonObject superfluousJsonObj = new JsonObject();
            superfluousJsonObj.put(STATUS, "OK");
            superfluousJsonObj.put(QUEUES, superfluousJsonArray);
            return superfluousJsonObj;
        }));
    }

    public Future<Set<Queue>> getQueueStatistics2(List<String> queueNames) {
        throw new UnsupportedOperationException/*TODO*/("not impl yet");
    }

    public void getQueuesSpeed(Message<JsonObject> ev, List<String> queueNames) {
        throw new UnsupportedOperationException/*TODO*/("not impl yet");
    }

    public void resetQueueStatistics(JsonArray queues, BiConsumer<Throwable, Void> todoQ93258hu38ErrorHandling) {
        throw new UnsupportedOperationException/*TODO*/("not impl yet");
    }

    private QueueInternal getQueue(String queueName) {
        assert mutxOwner == currentThread();
        QueueInternal queue = queues.get(queueName);
        if (queue == null) {
            queue = new QueueInternal();
            queue.name = queueName;
            queues.put(queue.name, queue);
        }
        return queue;
    }

    private void encode(Set<QueueInternal> queues, Writer dst) {
        ObjectMapper objectMapper = new ObjectMapper(); /*TODO move*/
        try {
            objectMapper.writeValue(dst, queues);
        } catch (IOException e) {
            throw new UncheckedIOException("TODO", e);
        }
    }

    private Set<QueueInternal> decode(byte[] body) {
        ObjectMapper objectMapper = new ObjectMapper()/*TODO*/;
        try {
            return objectMapper.readValue(body, new TypeReference<Set<QueueInternal>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException("TODO", e);
        }
    }


    private static class QueueInternal {
        public String name;
        public long newestSuccessEpchMs = -1;
        public long newestFailureEpchMs = -1;
        public int slowdownTimeSec = -1;
        public long backpressureDelayMs = -1;
    }

    public static class Queue {
        public String name;
        public long newestSuccessEpchMs = -1;
        public long newestFailureEpchMs = -1;
        public int slowdownTimeSec = -1;
        public long backpressureDelayMs = -1;
    }

}
