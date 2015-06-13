package org.projectreactor.bench.reactor;

import org.openjdk.jmh.annotations.*;
import reactor.bus.Event;
import reactor.core.Dispatcher;
import reactor.core.dispatch.RingBufferDispatcher;
import reactor.core.dispatch.RingBufferDispatcher3;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.BusySpinWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public class RingBufferDispatcherTailRecursionBenchmarks {

    @Measurement(iterations = 5, time = 1)
    @Warmup(iterations = 3, time = 1)
    @Fork(value = 1, jvmArgs = { "-Xmx1024m" })
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @State(Scope.Thread)
    @Timeout(time = 10)
    public static abstract class AbstractBenchmark {

        /**
         * Ring buffer size
         */
        @Param ( { "65536", "131072" } )
        public int BUFFER_SIZE;

        /**
         * Number of events dispatched recursively
         */
        @Param( { "131072", "196608", "262144" } )
        public int N;

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

                final Runnable initialPhase = new InitialPhase();

                final Runnable processingPhase = new ProcessingPhase();

                Runnable phase = initialPhase;

                class InitialPhase implements Runnable {
                    @Override
                    public void run() {
                        for (int i = 0; i < N; i++) {
                            doDispatch();
                        }

                        phase = processingPhase;
                    }
                }

                class ProcessingPhase implements Runnable {

                    int counter = 0;

                    @Override
                    public void run() {
                        counter++;
                        if (counter == N) {
                            counter = 0;
                            phase = initialPhase;
                            processed = true;
                        }
                    }
                }

                @Override
                public void accept(Event<?> event) {
                    phase.run();
                }
            };
        }

    }

    public static class RingBufferDispatcher_Benchmark extends AbstractBenchmark {

        @Override
        protected void doSetup() {
            dispatcher = new RingBufferDispatcher("ringBufferDispatcher", BUFFER_SIZE, null, ProducerType.MULTI,
                    new BusySpinWaitStrategy()
            );
        }

        @Override
        protected void doTearDown() {
            dispatcher.awaitAndShutdown(5, TimeUnit.SECONDS);
        }

    }

    public static class RingBufferDispatcher3_Benchmark extends AbstractBenchmark {

        @Override
        protected void doSetup() {
            dispatcher = new RingBufferDispatcher3("ringBufferDispatcher", BUFFER_SIZE, null, ProducerType.MULTI,
                    new BusySpinWaitStrategy());
        }

        @Override
        protected void doTearDown() {
            dispatcher.shutdown();
        }

    }

}
