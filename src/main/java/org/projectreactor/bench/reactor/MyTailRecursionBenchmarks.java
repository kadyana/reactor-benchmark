package org.projectreactor.bench.reactor;

import org.openjdk.jmh.annotations.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.bus.Event;
import reactor.core.Dispatcher;
import reactor.core.dispatch.RingBufferDispatcher;
import reactor.core.dispatch.RingBufferDispatcher2;
import reactor.core.dispatch.RingBufferDispatcher3;
import reactor.core.processor.RingBufferProcessor;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.BusySpinWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public class MyTailRecursionBenchmarks {

    public static final int N = 10000;

    public static int BACKLOG = 1024 * 1024;

    @Measurement(iterations = 5, time = 1)
    @Warmup(iterations = 5, time = 1)
    @Fork(value = 3, jvmArgs = { "-Xmx1024m" })
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @State(Scope.Thread)
    public static abstract class AbstractBenchmark {

        Event<?> event;

        volatile boolean processed;

        protected Consumer<Event<?>> consumer;

        protected Dispatcher dispatcher;

        @Setup
        public void setup() {
            event = Event.wrap("Hello World!");
            doSetup();
            createCustomer();
        }

        protected abstract void doSetup();

        @TearDown
        public void tearDown() {
            doTearDown();
        }

        protected abstract void doTearDown();

        @Benchmark
        public void benchmark() {
            processed = false;

            doDispatch();

            while (!processed) {
            }
        }

        private void doDispatch() {
            dispatcher.dispatch(
                    event,
                    consumer,
                    null
            );
        }

        protected void createCustomer() {
            this.consumer = new Consumer<Event<?>>() {

                int counter = 0;

                @Override
                public void accept(Event<?> event) {
                    counter++;
                    if(counter == N) {
                        processed = true;
                        counter = 0;
                    } else {
                        doDispatch();
                    }
                }

            };
        }

    }

    public static class RingBufferDispatcher_Benchmark extends AbstractBenchmark {

        @Override
        protected void doSetup() {
            dispatcher = new RingBufferDispatcher(
                    "dispatcher",
                    BACKLOG,
                    null,
                    ProducerType.MULTI,
                    new BusySpinWaitStrategy()
            );
        }

        @Override
        protected void doTearDown() {
            dispatcher.awaitAndShutdown(5, TimeUnit.SECONDS);
        }

    }

    public static class RingBufferDispatcher2_Benchmark extends AbstractBenchmark {

        @Override
        protected void doSetup() {
            dispatcher = new RingBufferDispatcher2("rbd2", BACKLOG, null, ProducerType.MULTI, new BusySpinWaitStrategy());
        }

        @Override
        protected void doTearDown() {
            dispatcher.shutdown();
        }

    }

    public static class RingBufferDispatcher3_Benchmark extends AbstractBenchmark {

        @Override
        protected void doSetup() {
            dispatcher = new RingBufferDispatcher3("rbd2", BACKLOG, null, ProducerType.MULTI, new BusySpinWaitStrategy());
        }

        @Override
        protected void doTearDown() {
            dispatcher.shutdown();
        }

    }
}
