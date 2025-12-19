package com.salescode.cdc.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.List;

/**
 * Root configuration object that contains all entities
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RootConfig {

    @JsonProperty("entities")
    private List<com.salescode.cdc.models.EntityConfig> entities;

    // Getter (Lombok @Data not processing, adding manually)
    public List<com.salescode.cdc.models.EntityConfig> getEntities() {
        return entities;
    }
}