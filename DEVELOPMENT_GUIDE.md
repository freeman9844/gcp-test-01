# Dataflow DLP Pipeline - Development & Reproduction Guide

This guide documents the critical configurations and best practices for building and running the Dataflow DLP pipeline.

---

## 1. Environment & Build Configuration

### Java Version
| Item | Value |
|------|-------|
| **Required** | Java 25 |
| **Build Tool** | Maven 3.9+ |

```xml
<properties>
    <maven.compiler.source>25</maven.compiler.source>
    <maven.compiler.target>25</maven.compiler.target>
    <beam.version>2.69.0</beam.version>
</properties>
```

### Fat JAR Creation (Uber-Jar)
Use `maven-shade-plugin` with signature file exclusion to prevent `SecurityException`:

```xml
<filter>
    <artifact>*:*</artifact>
    <excludes>
        <exclude>META-INF/*.SF</exclude>
        <exclude>META-INF/*.DSA</exclude>
        <exclude>META-INF/*.RSA</exclude>
    </excludes>
</filter>
```

---

## 2. DLP Implementation Best Practices

### InfoType Transformations
| InfoType | Masking Strategy | Config |
|----------|-----------------|--------|
| `KOREA_RRN` | Last 7 digits | `setNumberToMask(7)`, `setReverseOrder(true)` |
| `PHONE_NUMBER` | Last 4 digits | `setNumberToMask(4)`, `setReverseOrder(true)` |

### Detection Sensitivity
For mixed-text RRN detection, lower the threshold:
```java
InspectConfig.newBuilder()
    .setMinLikelihood(Likelihood.POSSIBLE)
    .build();
```

---

## 3. Robustness & Monitoring

### Metrics
| Metric | Description |
|--------|-------------|
| `rowsProcessed` | Successfully masked rows |
| `rowsFailed` | Failed DLP API calls |
| `dlpApiCalls` | Total API requests |

### Dead Letter Queue (DLQ)
- Use `TupleTag` to split **Success** and **Failure** streams.
- Failed rows go to `{output_table}_error`.
- **Note**: Create the error table before running (`CREATE_NEVER` disposition).

```sql
CREATE TABLE IF NOT EXISTS `project.dataset.table_error` 
LIKE `project.dataset.input_table`;
```

---

## 4. Execution

### Network Configuration
```bash
--network=jwlee-vpc-001 \
--subnetwork=regions/$REGION/subnetworks/jwlee-vpc-001
```

### Running the Pipeline
```bash
bash run_pipeline.sh
```

The script handles:
1. Maven build (`mvn clean package -DskipTests`)
2. Java environment setup
3. Dataflow job submission with all required parameters

---

## 5. Troubleshooting

| Issue | Solution |
|-------|----------|
| Network errors | Verify `--network` and `--subnetwork` match your VPC |
| `UnsupportedClassVersionError` | Ensure `pom.xml` targets correct Java version |
| `Invalid signature file digest` | Add META-INF exclusions to shade plugin |
| DLQ table not found | Pre-create error table with input schema |

---

## 6. Repository

- **GitHub**: [https://github.com/freeman9844/gcp-test-01](https://github.com/freeman9844/gcp-test-01)
- **Main Entry**: `com.example.dataflow.DlpPipeline`
