package org.swisspush.redisques;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.swisspush.redisques.util.DequeueStatistic;
import org.swisspush.redisques.util.QueueStatisticsCollector;

import java.util.*;
import java.util.function.Consumer;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptyList;
import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.util.RedisquesAPI.*;


public class QueueStatsService {

    private static final Logger log = getLogger(QueueStatsService.class);
    private final Vertx vertx;
    private final EventBus eventBus;
    private final String redisquesAddress;
    private final QueueStatisticsCollector queueStatisticsCollector;

    public QueueStatsService(Vertx vertx, EventBus eventBus, String redisquesAddress, QueueStatisticsCollector queueStatisticsCollector) {
        this.vertx = vertx;
        this.eventBus = eventBus;
        this.redisquesAddress = redisquesAddress;
        this.queueStatisticsCollector = queueStatisticsCollector;
    }

    public <CTX> void getQueueStats(CTX mCtx, GetQueueStatsMentor<CTX> mentor) {
        var req = new GetQueueStatsRequest<CTX>();
        req.mCtx = mCtx;
        req.mentor = mentor;
        fetchQueueNamesAndSize(req, ex1 -> {
            if (ex1 != null) { req.mentor.onError(ex1, req.mCtx); return; }
            // Prepare a list of queue names as it is needed to fetch retryDetails.
            req.queueNames = new ArrayList<>(req.queues.size());
            for (Queue q : req.queues) req.queueNames.add(q.name);
            fetchRetryDetails(req, ex2 -> {
                if (ex2 != null) { req.mentor.onError(ex2, req.mCtx); return; }
                mergeRetryDetailsIntoCollectedData(req, ex3 -> {
                    if (ex3 != null) { req.mentor.onError(ex3, req.mCtx); return; }
                    req.mentor.onQueueStatistics(req.queues, req.mCtx);
                });
            });
        });
    }

    private <CTX> void fetchQueueNamesAndSize(GetQueueStatsRequest<CTX> req, Consumer<Throwable> onDone) {
        String filter = req.mentor.filter(req.mCtx);
        JsonObject operation = buildGetQueuesItemsCountOperation(filter);
        eventBus.<JsonObject>request(redisquesAddress, operation, ev -> {
            if (ev.failed()) {
                req.mentor.onError(new Exception("eventBus.request()", ev.cause()), req.mCtx);
                return;
            }
            Message<JsonObject> msg = ev.result();
            JsonObject body = msg.body();
            String status = body.getString(STATUS);
            if (!OK.equals(status)) {
                req.mentor.onError(new Exception("Unexpected status " + status), req.mCtx);
                return;
            }
            JsonArray queuesJsonArr = body.getJsonArray(QUEUES);
            if (queuesJsonArr == null || queuesJsonArr.isEmpty()) {
                log.debug("result was {}, we return an empty result.", queuesJsonArr == null ? "null" : "empty");
                req.mentor.onQueueStatistics(emptyList(), req.mCtx);
                return;
            }
            boolean includeEmptyQueues = req.mentor.includeEmptyQueues(req.mCtx);
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
            int limit = req.mentor.limit(req.mCtx);
            if (limit != 0 && queues.size() > limit) queues = queues.subList(0, limit);
            req.queues = queues;
            onDone.accept(null);
        });
    }

    private <CTX> void fetchRetryDetails(GetQueueStatsRequest<CTX> req, Consumer<Throwable> onDone) {
        long begGetQueueStatsMs = currentTimeMillis();
        assert req.queueNames != null;
        queueStatisticsCollector.getQueueStatistics(req.queueNames).onComplete( ev -> {
            req.queueNames = null; // <- no longer needed
            long durGetQueueStatsMs = currentTimeMillis() - begGetQueueStatsMs;
            if (durGetQueueStatsMs > 42) log.debug("queueStatisticsCollector.getQueueStatistics() took {}ms", durGetQueueStatsMs);
            if (ev.failed()) {
                log.warn("queueStatisticsCollector.getQueueStatistics() failed. Fallback to empty result.", ev.cause());
                req.queuesJsonArr = new JsonArray();
                onDone.accept(null);
                return;
            }
            JsonObject queStatsJsonObj = ev.result();
            String status = queStatsJsonObj.getString(STATUS);
            if (!OK.equals(status)) {
                log.warn("queueStatisticsCollector.getQueueStatistics() responded '" + status + "'. Fallback to empty result.", ev.cause());
                req.queuesJsonArr = new JsonArray();
                onDone.accept(null);
                return;
            }
            req.queuesJsonArr = queStatsJsonObj.getJsonArray(QUEUES);
            onDone.accept(null);
        });
    }

    private <CTX> void mergeRetryDetailsIntoCollectedData(GetQueueStatsRequest<CTX> req, Consumer<Throwable> onDone) {
        // Setup a lookup table as we need to find by name further below.
        Map<String, JsonObject> detailsByName = new HashMap<>(req.queuesJsonArr.size());
        for (var it = (Iterator<JsonObject>) (Object) req.queuesJsonArr.iterator(); it.hasNext(); ) {
            JsonObject detailJson = it.next();
            String name = detailJson.getString(MONITOR_QUEUE_NAME);
            detailsByName.put(name, detailJson);
        }
        for (Queue queue : req.queues) {
            JsonObject detail = detailsByName.get(queue.name);
            if (detail == null) continue; // no details to enrich.
            JsonObject dequeueStatsJson = detail.getJsonObject(STATISTIC_QUEUE_DEQUEUESTATISTIC);
            if (dequeueStatsJson == null) continue; // no dequeue stats we could enrich
            DequeueStatistic dequeueStats = dequeueStatsJson.mapTo(DequeueStatistic.class);
            // Attach whatever details we got.
            queue.lastDequeueAttemptEpochMs = dequeueStats.lastDequeueAttemptTimestamp;
            queue.lastDequeueSuccessEpochMs = dequeueStats.lastDequeueSuccessTimestamp;
            queue.nextDequeueDueTimestampEpochMs = dequeueStats.nextDequeueDueTimestamp;
        }
        onDone.accept(null);
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


    private static class GetQueueStatsRequest<CTX> {
        private CTX mCtx;
        private GetQueueStatsMentor<CTX> mentor;
        private List<String> queueNames;
        private JsonArray queuesJsonArr;
        private List<Queue> queues;
    }


    public static class Queue {
        private String name;
        private Long size;
        private Long lastDequeueAttemptEpochMs;
        private Long lastDequeueSuccessEpochMs;
        private Long nextDequeueDueTimestampEpochMs;

        public String getName() { return name; }
        public long getSize() { return size; }
        public Long getLastDequeueAttemptEpochMs() { return lastDequeueAttemptEpochMs; }
        public Long getLastDequeueSuccessEpochMs() { return lastDequeueSuccessEpochMs; }
        public Long getNextDequeueDueTimestampEpochMs() { return nextDequeueDueTimestampEpochMs; }
    }


    /**
     * <p>Mentors fetching operations and so provides the fetcher the required
     * information. Finally it also receives the operations result.</p>
     *
     * @param <CTX>
     *     The context object of choice handled back to each callback so the mentor
     *     knows about what request the fetcher is talking.
     */
    public static interface GetQueueStatsMentor<CTX> {

        /**
         * <p>Returning true means that all queues will be present in the result. If
         * false, empty queues won't show up the result.</p>
         *
         * @param ctx  See {@link GetQueueStatsMentor}.
         */
        public boolean includeEmptyQueues( CTX ctx );

        public int limit( CTX ctx );

        public String filter( CTX ctx);

        public void onQueueStatistics(List<Queue> queues, CTX ctx);

        public void onError(Throwable ex, CTX ctx);
    }

}
