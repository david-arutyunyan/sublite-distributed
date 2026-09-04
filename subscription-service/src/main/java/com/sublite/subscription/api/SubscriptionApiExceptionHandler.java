package com.sublite.subscription.api;

import com.sublite.subscription.domain.PlanPriceNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class SubscriptionApiExceptionHandler {

    @ExceptionHandler(PlanPriceNotFoundException.class)
    ProblemDetail handleNotFound(PlanPriceNotFoundException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
    }
}
