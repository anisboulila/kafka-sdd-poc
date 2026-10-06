package com.example.kafkasddpoc.order;

public class OrderPublicationException extends RuntimeException {

    public OrderPublicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
