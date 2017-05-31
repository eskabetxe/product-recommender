package pro.boto.recommender.engine.controllers;

public class CustomError {

    private String errorMessage;

    public CustomError(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}