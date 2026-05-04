package com.evi.login.processor.transaction;

import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@Component
public class ReactiveTransactionExecutor implements TransactionExecutor {

    private final TransactionalOperator operator;

    public ReactiveTransactionExecutor(TransactionalOperator operator) {
        this.operator = operator;
    }

    @Override
    public <T> Mono<T> execute(Mono<T> mono) {
        return operator.execute(tx -> mono).next();
    }
}
