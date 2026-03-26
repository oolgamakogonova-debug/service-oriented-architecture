package com.example.shop.exception;

public class InvalidStateTransitionException extends RuntimeException {
    public InvalidStateTransitionException(String currentState, String attemptedAction) {
        super("Invalid state transition from " + currentState + " for action: " + attemptedAction);
    }
}