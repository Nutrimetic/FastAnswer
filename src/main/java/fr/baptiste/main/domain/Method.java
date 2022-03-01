package fr.baptiste.main.domain;

import java.io.Serializable;

public enum Method implements Serializable {
    GET("GET"),
    HEAD("HEAD"),
    POST("POST"),
    PUT("PUT"),
    TRACE("TRACE"),
    DEBUG("DEBUG"),
    DELETE("DELETE"),
    PROPFIND("PROPFIND"),
    TRACK("TRACK"),
    FLURP("FLURP"),
    OPTIONS("OPTIONS");

    private String value;

    Method() {
    }

    Method(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
