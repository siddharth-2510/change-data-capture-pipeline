package com.salescode.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class IcebergConfig {

    private String warehouse;

    @JsonProperty("catalog-name")
    private String catalogName;

    @JsonProperty("catalog-type")
    private String catalogType; // "glue" or "hadoop"

    private String database;
    private String table;

    @JsonProperty("glue-catalog-id")
    private String glueCatalogId; // AWS account ID (optional)
}
