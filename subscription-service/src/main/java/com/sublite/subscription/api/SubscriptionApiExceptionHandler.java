package com.sublite.subscription.api;

import com.sublite.subscription.domain.InvalidSubscriptionStateException;
import com.sublite.subscription.domain.PlanPriceNotFoundException;
import com.sublite.subscription.domain.SubscriptionNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class SubscriptionApiExceptionHandler {

    @ExceptionHandler({PlanPriceNotFoundException.class, SubscriptionNotFoundException.class})
    ProblemDetail handleNotFound(RuntimeException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(InvalidSubscriptionStateException.class)
    ProblemDetail handleConflict(InvalidSubscriptionStateException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
    }
}
