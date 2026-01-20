package org.swisspush.redisques.foo;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.CommandImpl;
import org.slf4j.Logger;
import org.swisspush.redisques.QueueState;
import org.swisspush.redisques.foo.RedisApiInterceptor.Ctx;
import org.swisspush.redisques.foo.RedisApiInterceptor.PerCmd;
import org.swisspush.redisques.foo.RedisApiInterceptor.RedisAccessMetric;
import org.swisspush.redisques.foo.RedisApiInterceptor.RedisAccessMetricPerCmd;
import org.swisspush.redisques.queue.QueueProcessingState;
import org.swisspush.redisques.scheduling.PeriodicSkipScheduler;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.vertx.core.Future.succeededFuture;


public interface FooDewohaew {


    public static <T> java.util.concurrent.Callable<T> callableOf(java.lang.Runnable r) {
        return () -> {
            r.run();
            return null;
        };
    }


    public static RedisAPI redisApiDecorate(Vertx vertx, @Nullable RedisAPI delegate) {
        var that = new Ctx();
        that.vertx = vertx;
        that.delegate = delegate;
        return that;
    }


    public static void setRedisApi(RedisAPI base, RedisAPI redisApi) {
        assert base instanceof Ctx : "base instanceof Ctx";
        assert redisApi != null : "redisApi != null";
        Ctx that = (Ctx) base;
        that.delegate = redisApi;
    }


    static Future<Response> onSend(Ctx that, Command cmd, String[] args) {
        if (that.delegate == null)
            return Future.failedFuture("MUST NOT use that instance until delegate got provided by calling `setRedisApi()`!");
        String cmdName = commandNameGet(that, cmd);
        that.mutx.lock();
        try {
            that.numRedisCalls += 1;
            PerCmd cmdCtx = that.currentMetricsByCmd.get(cmdName);
            cmdCtx.numCalled += 1;
        } finally {
            that.mutx.unlock();
        }
        return that.delegate.send(cmd, args);
    }


    static void onClose(Ctx that) {
        if (that.delegate != null) {
            that.delegate.close();
        }
        setMetricsObserver(that, 15, null);
    }


    static String commandNameGet(Ctx that, Command cmd) {
        Field field = that.commandName.updateAndGet(oldVal -> {
            if (oldVal != null) return oldVal;
            try {
                Field f = CommandImpl.class.getDeclaredField("command");
                f.setAccessible(true);
                return f;
            } catch (NoSuchFieldException e) {
                throw new UnsupportedOperationException("TODO", e);
            }
        });
        try {
            return (String) field.get(cmd);
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException("TODO", e);
        }
    }


    static void setMetricsObserver(Ctx that, int intervalSec, Consumer<RedisAccessMetric> metricsObserver) {
        if (intervalSec <= 0) throw new IllegalArgumentException(String.valueOf(intervalSec));
        Consumer<RedisAccessMetric> oldInstance;

        that.mutx.lock();
        try {
            oldInstance = that.metricObserver;
            that.metricObserver = metricsObserver;
            that.metricIntervalSec = intervalSec;
            that.metricTimerId = that.vertx.setTimer(that.metricIntervalSec * 1000L, t -> onMetricTimerExpired(that, t));
        } finally {
            that.mutx.unlock();
        }

        if (oldInstance != null) {
            Logger log = getLoggerFor(oldInstance);
            log.warn("This instance is NO LONGER the metricsObserver! Someone replaced it by: {}", metricsObserver);
        }
    }


    private static void onMetricTimerExpired(Ctx that, Long myTimerId) {
        long metricTimerId;
        long numRedisCalls;
        int metricIntervalSec;
        Consumer<RedisAccessMetric> metricObserver;
        HashMap<String, PerCmd> currentMetricsByCmd;
        long metricPeriodEndNs, metricPeriodBeginNs;
        var newMap = new HashMap<String, PerCmd>(32);

        that.mutx.lock();
        try {
            metricPeriodEndNs = System.nanoTime();
            /* get out the required data */
            metricTimerId = that.metricTimerId;
            metricObserver = that.metricObserver;
            metricIntervalSec = that.metricIntervalSec;
            numRedisCalls = that.numRedisCalls;
            currentMetricsByCmd = that.currentMetricsByCmd;
            metricPeriodBeginNs = that.metricPeriodBeginNs;
            /* update state where needed, to prepare next measuring period */
            that.metricPeriodBeginNs = metricPeriodEndNs;
            that.currentMetricsByCmd = newMap;
            newMap = null;
        } finally {
            that.mutx.unlock();
        }

        /* MUST NOT do anything. another observer (or a null-observer) has
         * replaced us meanwhile */
        if (myTimerId != metricTimerId || metricObserver == null) return;

        try {
            /* setup metrics for publication */
            float durationSec = nanosSmallDiff(metricPeriodEndNs, metricPeriodBeginNs) / 1_000_000_000.f;
            Map<String, RedisAccessMetricPerCmd> metricsPerCmd = new HashMap<>(currentMetricsByCmd.size());
            for (Map.Entry<String, PerCmd> e : currentMetricsByCmd.entrySet()) {
                String cmdName = e.getKey();
                PerCmd perCmd = e.getValue();
                long numCalled = perCmd.numCalled;
                metricsPerCmd.put(cmdName, new RedisAccessMetricPerCmd() {
                    @Override public String getCmdName(){ return cmdName; }
                    @Override public long getTimesCalled(){ return numCalled; }
                });
            }
            var metrics = new RedisAccessMetric() {
                @Override public float getSampleDurationSeconds() { return durationSec; }
                @Override public long getNumRedisCallsTotal() { return numRedisCalls; }
                @Override public Map<String, RedisAccessMetricPerCmd> getMetricPerCmd() { return metricsPerCmd; }
            };

            /* publish */
            try {
                if (metricObserver != null) metricObserver.accept(metrics);
            } catch (Exception ex) {
                var log = getLoggerFor(metricObserver);
                log.error("{}", ex.getMessage(), log.isDebugEnabled() ? ex : null);
            }
        } catch (Exception ex) {
            that.log.warn("{}", ex, that.log.isDebugEnabled() ? ex : null);
        }
        /* schedule next run */
        that.vertx.setTimer(metricIntervalSec * 1000L, t -> onMetricTimerExpired(that, t));
    }


    public static int clamp(int val, int min, int max) {
        return val < min ? min : val > max ? max : val;
    }


    public static org.slf4j.Logger getLoggerFor(Object obj) {
        String nm = obj.getClass().getName();
        int pos = nm.indexOf('$');
        if (pos != -1) nm = nm.substring(0, pos);
        return org.slf4j.LoggerFactory.getLogger(nm);
    }


    /**
     * Find smallest distance assuming integers overflow "like a circle".
     *
     * Computers cannot represent all existing integers. Due to how
     * integers are represented in java, they are not infinite but
     * more like a circle. Speak when we infinitely increment an integer,
     * it overflows and (usually) continues to walk around this
     * (imaginary) circle.
     *
     * This function takes two of those numbers on this circle and
     * returns the smallest distance to travel on the circle between
     * them. Here some examples:
     * <ul>
     *   <li>f(7, 13) = 6</li>
     *   <li>f(-7, +11) = 18</li>
     *   <li>f(LONG_MIN, LONG_MAX) = 1</li>
     *   <li>f(-9223372036854775805, 9223372036854775802) = 9</li>
     * </ul>
     *
     * This can be handy for example in conjunction with {@link System#nanoTime()}.
     * Because in case of overflows between measuring begNs and endNs a
     * simple subtraction would lead to uselessly large results. So we
     * can use it as:
     * <code>
     *   long begNs = System.nanoTime();
     *   long endNs = System.nanoTime();
     *   long durationNs = nanosSmallDiff(endNs, begNs);
     * </code>
     *
     * WARN: Do NOT use this if your distance can reach (LONG_MAX / 2).
     * Because in this case you would get wrong (too small) results.
     *
     * <a href="https://git.hiddenalpha.ch/UnspecifiedGarbage.git/tree/src/main/java/ch/hiddenalpha/unspecifiedgarbage/time/TimeUtils.java">Source</a>.
     *
     */
    public static long nanosSmallDiff( long a, long b ){
        return (a - b >= 0) ? a - b : b - a;
    }


    public static CtxAseheuth newCtxAseheuth(
            PeriodicSkipScheduler periodicSkipScheduler,
            Supplier<Map<String, QueueProcessingState>> getQueues,
            Supplier<String> getMyVerticleId,
            long periodMs
    ) {
        assert periodMs > 0 : periodMs + " > 0";
        var that = new CtxAseheuth();
        that.executor = null; /* TODO get via ctor or factory, as of needed */
        that.periodicSkipScheduler = periodicSkipScheduler;
        that.getMyVerticleId = getMyVerticleId;
        that.getQueues = getQueues;
        that.periodMs = periodMs;
        /* TODO set other fields */
        return that;
    }


    public static void registerActiveQueueRegistrationRefresh(CtxAseheuth that) {
        that.periodicSkipScheduler.setPeriodic(that.periodMs, "registerActiveQueueRegistrationRefresh",
                onDone -> onPeriodicTriggerHasFired(that, onDone));
    }


    /**
     * Adapt `callback` to `Future` API.
     */
    private static void onPeriodicTriggerHasFired(CtxAseheuth that, Runnable onDone) {
        onPeriodicTriggerHasFired(that).onComplete((AsyncResult<Void> ev) -> {
            if (ev.failed()) {
                Throwable ex = ev.cause();
                Logger log = null/*TODO*/;
                log.error("{}", ex.getMessage(), log.isDebugEnabled() ? ex : null);
                /* must NOT return, as we've to call `onDone' in ANY case */
            }
            onDone.run();
        });
    }


    private static Future<Void> onPeriodicTriggerHasFired(CtxAseheuth that) {
        Logger log = null/*TODO*/;
        return succeededFuture().<List<Future<Void>>>compose((nil) -> {
            Map<String, QueueProcessingState> qpStateByQueueName;
            qpStateByQueueName = that.getQueues.get();
            var tasks = new ArrayList<Callable<Future<Void>>>(qpStateByQueueName.size());
            for (var e : qpStateByQueueName.entrySet()) {
                String queueName = e.getKey();
                QueueProcessingState qpState = e.getValue();
                QueueState qState = qpState.getState();
                if (qState != QueueState.CONSUMING) {
                    log.trace("nothing to be done (state '{}') for '{}'", qState, queueName);
                    continue;
                }
                tasks.add(() -> refreshQueueConsumerGivenItsMine(that, queueName, qpState));
            }
            return that.executor.executeDespiteFail(tasks.iterator());
        }).<Void>compose((List<Future<Void>> results) -> {
            /* all tasks done. Inspect outcome */
            int numOk = 0, numFail = 0;
            for (Future<Void> result : results) {
                if (result.failed()) {
                    Throwable ex = result.cause();
                    /* In reality this is a `warn`-ing. But unfortunately SDCISA-22567 tells
                     * us to conceal errors more (for monetary reasons). */
                    log.info("{}", ex.getMessage(), log.isDebugEnabled() ? ex : null);
                    numFail += 1;
                    continue;
                }
                numOk += 1;
            }
            if (numFail > 0) {
                log.info("{} out of {} did fail (see earlier logs)", numFail, numOk + numFail);
            }
            throw new UnsupportedOperationException("TODO");
        });
    }


    private static Future<Void> refreshQueueConsumerGivenItsMine(CtxAseheuth that, String queueName, QueueProcessingState qpState) {
        Logger log = null/*TODO*/;
        return succeededFuture().<Boolean>compose((nil) -> {
            return askRedisIfImTheQueueConsumer(that, queueName);
        }).<Void>compose((Boolean iAmTheConsumer) -> {
            if (iAmTheConsumer) {
                log.debug("RedisQues Periodic consumer refresh for '{}'", queueName);
                return refreshRegistration(queueName);
            }else{
                log.debug("RedisQues Removing queue '{}' from list", queueName);
                queueConsumerRunner.getMyQueues().remove(queueName);
                queueStatsService.dequeueStatisticRemoveFromLocal(queueName);
                return queueStatisticsCollector.resetQueueFailureStatistics(queueName);
            }
        }).<Void>compose((Void nil) -> {
            metrics.perQueueMetricsRefresh(queueName);
            return updateTimestamp(queueName);
        }).<Void>compose((Void nil) -> {
            ;
        });
    }


    static Future<Boolean> askRedisIfImTheQueueConsumer(CtxAseheuth that, String queueName) {
        return succeededFuture().<Response>compose((nil) -> {
            String consumerKey = that.getMyConsumerNameByQueueName.apply(queueName);
            return that.redis.get(consumerKey);
        }).<Boolean>compose((Response rsp) -> {
            String otherVerticleUid = (rsp == null) ? null : rsp.toString();
            String myVerticleId = that.getMyVerticleId.get();
            return succeededFuture(myVerticleId != null && Objects.equals(myVerticleId, otherVerticleUid));
        });
    }


    private static Future<Void> refreshRegistration(CtxAseheuth that, String queueName) {
        /*
         * TODO WTF?!? Now (LONG AFTER OUR CHECK) we just blindly set the new
         *   value?!? So this means we just set values by good luck, and are
         *   affected by all kind of race-conditions from other client requests
         *   which probably interfer with ours? How does this not cause more
         *   problems?!?
         */
        Logger log = null/*TODO*/;
        Vertx vertx = null/*TODO*/;
        return succeededFuture().<Void>compose((nil) -> {
            String consumerKey = that.getMyConsumerNameByQueueName.apply(queueName);
            int consumerLockTime;
            log.debug("Refreshing registration of queue consumer {}, expire in {} s", queueName, consumerLockTime);
            /* TODO WTF?!? Why do we trigger **ASYNC** operations from a worker
             *   thread? Doesn't make a lot of sense. Especially because there's
             *   no comment explaining why this is necessary. So please replace
             *   this T0D0 with a meaningful explanation. */
            /* TODO WARN: Searching the internetz, it is said that `redis` is **NOT**
             *   thread-safe! So calling redis APIs from worker-threads, potentially
             *   even triggers concurrency issues. */
            return vertx.<Void>executeBlocking((Promise<Void> p) -> {
                that.redis.expire(Arrays.asList(consumerKey, String.valueOf(consumerLockTime))).onComplete((AsyncResult<Response> ev) -> {
                    if (ev.failed()) {
                        p.tryFail(ev.cause());
                        return;
                    }
                    p.tryComplete();
                });
            }, false);
        }).<Void>compose((Void nil) -> {
            getQueueConsumerRunner().updateLastRefreshRegistrationTimeStamp(queueName);
        });
    }


}
