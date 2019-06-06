package zzw.bufferconsumer;

import static java.util.concurrent.TimeUnit.DAYS;

/**
 * @author zhangzhewei
 * Created on 2019-06-04
 */
public interface ConsumerStrategy {

    ConsumerCursor canConsume(long lastConsumeTimestamp, long changeCount);

    /**
     * cursor
     */
    class ConsumerCursor {
        private static final ConsumerCursor EMPTY = new ConsumerCursor(true, DAYS.toMillis(1));
        private final boolean doConsume;
        private final long nextPeriod;

        private ConsumerCursor(boolean doConsume, long nextPeriod) {
            this.doConsume = doConsume;
            this.nextPeriod = nextPeriod;
        }

        public static ConsumerCursor cursor(boolean doConsumer, long nextPeriod) {
            return new ConsumerCursor(doConsumer, nextPeriod);
        }

        public static ConsumerCursor empty() {
            return EMPTY;
        }

        public boolean isDoConsume() {
            return doConsume;
        }

        public long getNextPeriod() {
            return nextPeriod;
        }
    }

}
