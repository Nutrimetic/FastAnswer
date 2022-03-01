package fr.baptiste.main.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

public class Http implements Serializable {
    private String client; //the client IP
    private String datetime; //the time the request was received
    private Method method;
    private String request; //the request line from the client. ("/ HTTP/1.0")
    private int status; //the status code sent from the server to the client (200, 404 etc.)
    private String size; //the size of the response to the client (in bytes)
    private String referrer; //contain the URL of the page from which this request was initiated or "-" otherwise
    private String useragent; //the browser identification string

    @JsonCreator
    public Http(@JsonProperty("client") String client,
                @JsonProperty("datetime") String datetime,
                @JsonProperty("method") Method method,
                @JsonProperty("request") String request,
                @JsonProperty("status") int status,
                @JsonProperty("size") String size,
                @JsonProperty("referrer") String referrer,
                @JsonProperty("useragent") String useragent) {
        this.client = client;
        this.datetime = datetime;
        this.method = method;
        this.request = request;
        this.status = status;
        this.size = size;
        this.referrer = referrer;
        this.useragent = useragent;
    }

    public String getClient() {
        return client;
    }

    public String getDatetime() {
        return datetime;
    }

    public Method getMethod() {
        return method;
    }

    public String getRequest() {
        return request;
    }

    public int getStatus() {
        return status;
    }

    public String getSize() {
        return size;
    }

    public String getReferrer() {
        return referrer;
    }

    public String getUseragent() {
        return useragent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Http)) return false;
        Http http = (Http) o;
        return status == http.status &&
                client.equals(http.client) &&
                datetime.equals(http.datetime) &&
                method == http.method &&
                Objects.equals(request, http.request) &&
                size.equals(http.size) &&
                referrer.equals(http.referrer) &&
                useragent.equals(http.useragent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(client, datetime, method, request, status, size, referrer, useragent);
    }

    @Override
    public String toString() {
        return "Http{" +
                "client='" + client + '\'' +
                ", datetime='" + datetime + '\'' +
                ", method=" + method +
                ", request='" + request + '\'' +
                ", status=" + status +
                ", size='" + size + '\'' +
                ", referrer='" + referrer + '\'' +
                ", useragent='" + useragent + '\'' +
                '}';
    }
}
