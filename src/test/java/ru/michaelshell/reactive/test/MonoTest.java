package ru.michaelshell.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

@Slf4j
class MonoTest {

    @BeforeAll
    static void init() {
        BlockHound.install();
    }

    @Test
    @Disabled("blockhound tested")
    void blockhound() {
        Mono.delay(Duration.ofMillis(1))
                .doOnNext(it -> {
                    try {
                        Thread.sleep(10);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .block(); // should throw an exception about Thread.sleep
    }

    @Test
    void monoSubscribe() {
        Mono<String> mono = Mono.just("Michael").log();

        StepVerifier.create(mono)
                .expectNext("Michael")
                .verifyComplete();
    }

    @Test
    void monoSubscriberConsumer() {
        Mono<String> mono = Mono.just("Michael").log();
        mono.subscribe(s -> log.info("value: " + s));

        StepVerifier.create(mono)
                .expectNext("Michael")
                .verifyComplete();
    }

    @Test
    void monoSubscriberConsumerError() {
        Mono<String> mono = Mono.just("Michael")
                .handle((s, sink) -> sink.error(new RuntimeException("Error message!!")));

        mono.subscribe(s -> log.info("value: " + s), Throwable::printStackTrace);

        log.info("----------------------------------------");
        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void monoSubscriberConsumerCompleted() {
        Mono<String> mono = Mono.just("Michael");

        mono.subscribe(s -> log.info("value: " + s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED"));

        log.info("----------------------------------------");
        StepVerifier.create(mono)
                .expectNext("Michael")
                .verifyComplete();
    }

    @Test
    void monoSubscriberConsumerCompletedSubscription() {
        Mono<String> mono = Mono.just("Michael").log();

        mono.subscribe(s -> log.info("value: " + s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED"),
                Subscription::cancel);

        log.info("----------------------------------------");
        StepVerifier.create(mono)
                .expectNext("Michael")
                .verifyComplete();
    }

    @Test
    void monoDoOnMethods() {
        Mono<String> mono = Mono.just("Michael").log()
                .doOnSubscribe(subscription -> log.info("Subscribed"))
                .doOnRequest(l -> log.info("doOnRequest Info"))
                .doOnNext(s -> log.info("DoOnNext Value: {} ", s));

        mono.subscribe(s -> log.info("value: " + s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED")
               );

        log.info("----------------------------------------");
        StepVerifier.create(mono)
                .expectNext("Michael")
                .verifyComplete();
    }

    @Test
    void monoDoOnError() {
        Mono<Object> error = Mono.error(new RuntimeException("Runtime ex"))
                .doOnError(e -> log.info("error message: {}", e.getMessage())).log();

        StepVerifier.create(error)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void monoDoOnErrorResume() {
        Mono<Object> error = Mono.error(new RuntimeException("Runtime ex"))
                .doOnError(e -> log.info("error message: {}", e.getMessage()))
                .onErrorResume(e -> {
                    log.info("Error Resume");
                    return Mono.just("afterErrorResume");
                })
                .log();

        StepVerifier.create(error)
                .expectNext("afterErrorResume")
                .verifyComplete();
    }

    @Test
    void monoDoOnErrorReturn() {
        Mono<Object> error = Mono.error(new RuntimeException("Runtime ex"))
                .doOnError(e -> log.info("error message: {}", e.getMessage()))
                .onErrorReturn("RETURN")
                .onErrorResume(e -> {
                    log.info("Error Resume");
                    return Mono.just("afterErrorResume");
                })
                .log();

        StepVerifier.create(error)
                .expectNext("RETURN")
                .verifyComplete();
    }

}
