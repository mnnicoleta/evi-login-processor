package com.evi.login.processor.transaction;

import reactor.core.publisher.Mono;

public interface TransactionExecutor {
    <T> Mono<T> execute(Mono<T> mono);
}
