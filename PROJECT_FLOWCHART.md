# Change Data Capture Pipeline - Project Flowchart

## High-Level Architecture Overview

```mermaid
flowchart TB
    subgraph External["External Systems"]
        KAFKA["☁️ Apache Kafka<br/>entity-change-events"]
        MINIO["💾 MinIO (S3)<br/>Object Storage"]
    end

    subgraph FlinkApp["Apache Flink Application"]
        MAIN["Main.java<br/>Entry Point"]
        
        subgraph Config["Configuration Layer"]
            CONFIG_LOADER["ConfigLoader"]
            APP_CONFIG["AppConfig"]
            KAFKA_CONFIG["KafkaConfig"]
            S3_CONFIG["S3Config"]
            ICEBERG_CONFIG["IcebergConfig"]
        end
        
        subgraph Sources["Source Layer"]
            KAFKA_BUILDER["KafkaSourceBuilder"]
            KAFKA_SOURCE["KafkaSource<ObjectNode>"]
        end
        
        subgraph Transform["Transformation Layer"]
            HEADER_TRANS["OrderHeaderTransformer"]
            DETAILS_TRANS["OrderDetailsTransformer"]
        end
        
        subgraph Sink["Sink Layer"]
            ICEBERG_UTIL["IcebergUtil"]
            SINK_BUILDER["IcebergSinkBuilder"]
            HEADER_MAPPER["OrderHeaderMapper"]
            DETAILS_MAPPER["OrderDetailsMapper"]
        end
    end

    subgraph Storage["Iceberg Tables"]
        ORDERS_TABLE["📊 db.orders<br/>Order Headers"]
        DETAILS_TABLE["📊 db.order_details<br/>Order Line Items"]
    end

    KAFKA --> KAFKA_SOURCE
    KAFKA_SOURCE --> HEADER_TRANS
    KAFKA_SOURCE --> DETAILS_TRANS
    HEADER_TRANS --> HEADER_MAPPER
    DETAILS_TRANS --> DETAILS_MAPPER
    HEADER_MAPPER --> ORDERS_TABLE
    DETAILS_MAPPER --> DETAILS_TABLE
    ORDERS_TABLE --> MINIO
    DETAILS_TABLE --> MINIO
```

---

## Detailed Data Flow

```mermaid
flowchart LR
    subgraph Input["1️⃣ Input"]
        KT["Kafka Topic<br/>entity-change-events"]
    end
    
    subgraph Ingest["2️⃣ Ingestion"]
        KS["KafkaSource<br/>JSON Deserialization"]
        WM["Watermark Strategy<br/>(No Watermarks)"]
    end
    
    subgraph Parse["3️⃣ Parsing"]
        OBJ["ObjectNode<br/>Raw JSON Events"]
        FEA["features[]<br/>Array of Orders"]
    end
    
    subgraph Transform["4️⃣ Transformation"]
        direction TB
        OH["Order Header<br/>Transformer"]
        OD["Order Details<br/>Transformer"]
    end
    
    subgraph Convert["5️⃣ Row Conversion"]
        HM["Header Mapper<br/>ObjectNode → RowData"]
        DM["Details Mapper<br/>ObjectNode → RowData"]
    end
    
    subgraph Output["6️⃣ Output"]
        IT1["Iceberg Table<br/>db.orders"]
        IT2["Iceberg Table<br/>db.order_details"]
    end
    
    subgraph Store["7️⃣ Storage"]
        S3["MinIO S3<br/>s3a://warehouse/db/"]
    end

    KT --> KS --> WM --> OBJ --> FEA
    FEA --> OH --> HM --> IT1 --> S3
    FEA --> OD --> DM --> IT2 --> S3
```

---

## Component Interaction Diagram

```mermaid
sequenceDiagram
    participant Main
    participant ConfigLoader
    participant FlinkEnv as Flink Environment
    participant KafkaBuilder as KafkaSourceBuilder
    participant Kafka
    participant OrderHeaderTx as OrderHeaderTransformer
    participant OrderDetailsTx as OrderDetailsTransformer
    participant IcebergUtil
    participant SinkBuilder as IcebergSinkBuilder
    participant MinIO

    Main->>ConfigLoader: loadConfig("application.yaml")
    ConfigLoader-->>Main: AppConfig
    
    Main->>FlinkEnv: getExecutionEnvironment()
    Main->>FlinkEnv: setParallelism(1)
    Main->>FlinkEnv: enableCheckpointing(10s)
    
    Main->>KafkaBuilder: build(kafkaConfig)
    KafkaBuilder-->>Main: KafkaSource<ObjectNode>
    
    Main->>FlinkEnv: fromSource(kafkaSource)
    FlinkEnv-->>Main: DataStream<ObjectNode>
    
    loop Process Kafka Messages
        Kafka->>FlinkEnv: JSON Events
        FlinkEnv->>OrderHeaderTx: flatMap(event)
        OrderHeaderTx-->>FlinkEnv: Order Header Records
        FlinkEnv->>OrderDetailsTx: flatMap(event)
        OrderDetailsTx-->>FlinkEnv: Order Detail Records
    end
    
    Main->>IcebergUtil: ordersTableLoader()
    Main->>IcebergUtil: orderDetailsTableLoader()
    
    Main->>SinkBuilder: createOrderHeaderSink()
    Main->>SinkBuilder: createOrderDetailsSink()
    
    SinkBuilder->>MinIO: Write Parquet Files (Iceberg)
    
    Main->>FlinkEnv: execute("Pipeline")
```

---

## Package Structure

```mermaid
flowchart TB
    subgraph root["com.salescode"]
        MAIN["Main.java<br/>🚀 Entry Point"]
        
        subgraph config["config/"]
            AC["AppConfig.java"]
            CL["ConfigLoader.java"]
            KC["KafkaConfig.java"]
            IC["IcebergConfig.java"]
            SC["S3Config.java"]
        end
        
        subgraph kafka["kafka/"]
            KSB["KafkaSourceBuilder.java"]
        end
        
        subgraph transformer["transformer/"]
            OHT["OrderHeaderTransformer.java"]
            ODT["OrderDetailsTransformer.java"]
        end
        
        subgraph sink["sink/"]
            ISB["IcebergSinkBuilder.java"]
        end
        
        subgraph iceberg["iceberg/"]
            IU["IcebergUtil.java"]
            CIT["CreateIcebergTables.java"]
        end
    end

    MAIN --> config
    MAIN --> kafka
    MAIN --> transformer
    MAIN --> sink
    sink --> iceberg
```

---

## Data Transformation Flow

```mermaid
flowchart TD
    subgraph Input["Kafka JSON Message"]
        RAW["{<br/>  features: [<br/>    { id, orderNumber, orderDetails: [...] }<br/>  ]<br/>}"]
    end
    
    subgraph Split["Split Processing"]
        PARENT["Parent Order Data<br/>(Header Fields)"]
        CHILD["Child Order Details<br/>(Line Items)"]
    end
    
    subgraph HeaderFields["Order Header Fields"]
        H1["id, lob, order_number"]
        H2["bill_amount, net_amount, total_amount"]
        H3["outletcode, supplierid, channel"]
        H4["status, beat, hierarchy"]
        H5["extended_attributes, discount_info"]
    end
    
    subgraph DetailsFields["Order Details Fields"]
        D1["id, order_id (FK), skucode"]
        D2["initial_amount, initial_quantity"]
        D3["mrp, net_amount, bill_amount"]
        D4["price, case_price, sales_value"]
        D5["discount_info, product_info"]
    end
    
    subgraph Output["Iceberg Tables"]
        ORDERS["📊 db.orders<br/>(Parquet in MinIO)"]
        ORDER_DETAILS["📊 db.order_details<br/>(Parquet in MinIO)"]
    end

    RAW --> PARENT
    RAW --> CHILD
    PARENT --> H1 --> ORDERS
    PARENT --> H2 --> ORDERS
    PARENT --> H3 --> ORDERS
    PARENT --> H4 --> ORDERS
    PARENT --> H5 --> ORDERS
    CHILD --> D1 --> ORDER_DETAILS
    CHILD --> D2 --> ORDER_DETAILS
    CHILD --> D3 --> ORDER_DETAILS
    CHILD --> D4 --> ORDER_DETAILS
    CHILD --> D5 --> ORDER_DETAILS
```

---

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Streaming** | Apache Flink | Real-time stream processing |
| **Source** | Apache Kafka | Message broker / event streaming |
| **Format** | JSON | Data serialization format |
| **Table Format** | Apache Iceberg | ACID-compliant table format |
| **Storage** | MinIO (S3-compatible) | Object storage backend |
| **File Format** | Parquet | Columnar storage format |
| **Config** | YAML | Application configuration |
| **Build** | Maven | Dependency management |

---

## Key Features

- ✅ **Real-time CDC Processing** - Continuous streaming from Kafka
- ✅ **Dual Output Streams** - Separate Order Headers and Order Details tables
- ✅ **Checkpointing** - 10-second intervals for fault tolerance
- ✅ **Iceberg Integration** - ACID transactions and time-travel queries
- ✅ **S3-Compatible Storage** - MinIO for local development
- ✅ **Schema Transformation** - JSON to structured RowData conversion
