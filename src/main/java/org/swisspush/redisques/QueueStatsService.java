package org.swisspush.redisques;

import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.currentTimeMillis;
import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.util.RedisquesAPI.*;


public class QueueStatsService {

    private static final Logger log = getLogger(QueueStatsService.class);
    private final EventBus eventBus;
    private final String redisquesAddress;
    private final QueueStatisticsCollector queueStatisticsCollector;

    public QueueStatsService(EventBus eventBus, String redisquesAddress, QueueStatisticsCollector queueStatisticsCollector) {
        this.eventBus = eventBus;
        this.redisquesAddress = redisquesAddress;
        this.queueStatisticsCollector = queueStatisticsCollector;
    }

    public <CTX> void getQueueStats(CTX mCtx, GetQueueStatsMentor<CTX> mentor) {
        var p1 = Promise.<List<Queue>>promise();
        fetchQueueNamesAndSize(mentor.filter(mCtx), mentor.includeEmptyQueues(mCtx), mentor.limit(mCtx), p1);
        p1.future().onComplete( ev1 -> {
            if (ev1.failed()) throw new UnsupportedOperationException/*TODO*/("not impl yet", ev1.cause());
            List<Queue> queues = ev1.result();
            var queueNames = new ArrayList<String>(queues.size());
            for (Queue q : queues) queueNames.add(q.name);
            var p2 = Promise.<JsonArray>promise();
            fetchMoreFunkyStuff(queueNames, p2);
            p2.future().onComplete( ev2 -> {
                if (ev2.failed()) throw new UnsupportedOperationException/*TODO*/("not impl yet", ev2.cause());
                throw new UnsupportedOperationException/*TODO*/("not impl yet");
            });
        });
    }

    private void fetchQueueNamesAndSize(String filter, boolean includeEmptyQueues, int limit, Promise<List<Queue>> onDone) {
        JsonObject operation = buildGetQueuesItemsCountOperation(filter);
        eventBus.<JsonObject>request(redisquesAddress, operation, ev -> {
            if (ev.failed()) {
                throw new UnsupportedOperationException/*TODO*/("not impl yet", ev.cause());
            }
            Message<JsonObject> msg = ev.result();
            JsonObject body = msg.body();
            String status = body.getString(STATUS);
            if( !OK.equals(status) ) throw new UnsupportedOperationException/*TODO*/("not impl yet");
            JsonArray queuesJsonArr = body.getJsonArray(QUEUES);
            if( queuesJsonArr == null || queuesJsonArr.isEmpty() ) throw new UnsupportedOperationException/*TODO*/("not impl yet");
            List<Queue> queues = new ArrayList<>(queuesJsonArr.size());
            for (var it = queuesJsonArr.iterator(); it.hasNext(); ) {
                JsonObject queueJson = (JsonObject) it.next();
                String name = queueJson.getString(MONITOR_QUEUE_NAME);
                Long size = queueJson.getLong(MONITOR_QUEUE_SIZE);
                // No need to process empty queues any further if caller is not interested
                // in them anyway.
                if (!includeEmptyQueues && (size == null || size == 0)) continue;
                Queue queue = new Queue();
                queue.name = name;
                queue.size = size;
                queues.add(queue);
            }
            queues.sort(this::compareLargestFirst);
            // Only the part with the most filled queues got requested. Get rid of
            // all shorter queues then.
            if (limit != 0 && queues.size() > limit) queues = queues.subList(0, limit);
            onDone.complete(queues);
        });
    }

    private void fetchMoreFunkyStuff(List<String> queueNames, Promise<JsonArray> onDone) {
        long begGetQueueStatsMs = currentTimeMillis();
        queueStatisticsCollector.getQueueStatistics(queueNames).onComplete( ev -> {
            long durGetQueueStatsMs = currentTimeMillis() - begGetQueueStatsMs;
            if (durGetQueueStatsMs > 42) log.debug("queueStatisticsCollector.getQueueStatistics() took {}ms", durGetQueueStatsMs);
            if (ev.failed()) throw new UnsupportedOperationException/*TODO*/("not impl yet");
            JsonObject queStatsJsonObj = ev.result();
            String status = queStatsJsonObj.getString(STATUS);
            if (!OK.equals(status)) throw new UnsupportedOperationException/*TODO*/("not impl yet");
            JsonArray queuesJsonArr = queStatsJsonObj.getJsonArray(QUEUES);
            if (queuesJsonArr.isEmpty()) throw new UnsupportedOperationException/*TODO*/("not impl yet");
            onDone.complete(queuesJsonArr);
        });
    }

    private int compareLargestFirst(Queue aq, Queue bq) {
        if (aq.size == null && bq.size == null) return 0;
        if (aq.size == null) return -1;
        if (bq.size == null) return +1;
        long as = aq.size, bs = bq.size;
        if (as > bs) return -1;
        if (as < bs) return +1;
        assert as == bs : as +", "+ bs;
        return 0;
    }


    private static class Queue {
        String name;
        Long size;
    }


    public static interface GetQueueStatsMentor<CTX> {

        public boolean includeEmptyQueues( CTX ctx );

        public int limit( CTX ctx );

        public String filter( CTX ctx);
    }

}
