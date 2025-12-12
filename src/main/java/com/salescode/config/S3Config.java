package com.salescode.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data

public class S3Config {

    private String endpoint;

    @JsonProperty("access-key")
    private String accessKey;

    @JsonProperty("secret-key")
    private String secretKey;

    @JsonProperty("path-style-access")
    private boolean pathStyleAccess;
}
