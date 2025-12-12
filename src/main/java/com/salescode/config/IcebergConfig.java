package com.salescode.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class IcebergConfig {

    private String warehouse;

    @JsonProperty("catalog-name")
    private String catalogName;

    private String database;
    private String table;
}

