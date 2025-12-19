package com.salescode.cdc.models;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;
import java.util.List;

/**
 * Configuration for a field with support for nested sub-fields.
 * This is a recursive structure - sub-fields can contain their own sub-fields.
 */
public class FieldConfig implements Serializable {

    /**
     * Path to the field in JSON (e.g., "orderId", "orderDetails")
     */
    @JsonProperty("field-path")
    private String fieldPath;

    /**
     * Data type of the field (e.g., "String", "List", "Object", "Integer", "Double")
     */
    @JsonProperty("type")
    private String type;

    /**
     * Sub-fields for complex types (List or Object).
     * Null for simple types like String, Integer.
     * For List/Object types, contains the nested field structure.
     */
    @JsonProperty("sub-fields")
    private List<FieldConfig> subFields;

    /**
     * Default constructor
     */
    public FieldConfig() {
    }

    /**
     * Constructor with all fields
     */
    public FieldConfig(String fieldPath, String type, List<FieldConfig> subFields) {
        this.fieldPath = fieldPath;
        this.type = type;
        this.subFields = subFields;
    }

    /**
     * Getter for fieldPath
     */
    public String getFieldPath() {
        return fieldPath;
    }

    /**
     * Setter for fieldPath
     */
    public void setFieldPath(String fieldPath) {
        this.fieldPath = fieldPath;
    }

    /**
     * Getter for type
     */
    public String getType() {
        return type;
    }

    /**
     * Setter for type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Getter for subFields
     */
    public List<FieldConfig> getSubFields() {
        return subFields;
    }

    /**
     * Setter for subFields
     */
    public void setSubFields(List<FieldConfig> subFields) {
        this.subFields = subFields;
    }

    /**
     * Check if this field has sub-fields
     */
    public boolean hasSubFields() {
        return subFields != null && !subFields.isEmpty();
    }

    /**
     * Check if this is a simple field (no sub-fields)
     */
    public boolean isSimpleField() {
        return !hasSubFields();
    }

    /**
     * Check if this is a List type
     */
    public boolean isListType() {
        return "List".equalsIgnoreCase(type);
    }

    /**
     * Check if this is an Object type
     */
    public boolean isMapType() {
        return "Map".equalsIgnoreCase(type);
    }
}
