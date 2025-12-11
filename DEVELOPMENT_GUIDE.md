# Dataflow DLP Pipeline - Development & Reproduction Guide

This guide documents the critical configurations and best practices required to successfully build and run the Dataflow DLP pipeline, based on the debugging and development session.

## 1. Environment & Build Configuration

### Java Version Compatibility
*   **Requirement**: Dataflow workers currently support up to **Java 21 (LTS)**.
*   **Configuration**: Ensure `pom.xml` targets Java 21.
    ```xml
    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
    </properties>
    ```

### Fat JAR Creation (Uber-Jar)
*   **Requirement**: Dataflow execution requires a standalone JAR containing all dependencies.
*   **Plugin**: Use `maven-shade-plugin`.
*   **Critical Fix**: Exclude signature files to prevent `SecurityException` (`Invalid signature file digest`).
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

## 2. DLP Implementation Best Practices

### InfoType Transformations
Instead of using a single default mask, use `InfoTypeTransformations` to apply specific rules per data type.

*   **Korea RRN**: Mask last 7 digits (`******`).
*   **Phone Number**: Mask last 4 digits (`010-1234-****`).
    *   *Config*: `setNumberToMask(4)`, `setReverseOrder(true)`.

### Detection Sensitivity
*   **Issue**: Default detection may miss RRNs in mixed text.
*   **Fix**: Lower the detection threshold in `InspectConfig`.
    *   `setMinLikelihood(Likelihood.POSSIBLE)`

## 3. Robustness & Monitoring

### Metrics
Implement `Counter` metrics to track pipeline health in the Dataflow console.
*   `rowsProcessed`: Successful masking.
*   `rowsFailed`: Errors during DLP API calls.
*   `dlpApiCalls`: Total API requests made.

### Error Handling (Dead Letter Queue)
Do not fail the entire batch on a single error.
*   Use `TupleTag` to split output into **Success** and **Failure** streams.
*   Route failed rows to a separate BigQuery table (e.g., `_error` suffix) for analysis.

## 4. Execution

### Network Configuration
Dataflow jobs in specific regions often require explicit VPC network configuration.
*   **Command**:
    ```bash
    --network=jwlee-vpc-001 \
    --subnetwork=regions/$REGION/subnetworks/jwlee-vpc-001
    ```

### Running the Pipeline
Use the provided `run_pipeline.sh` script which encapsulates:
1.  Setting `JAVA_HOME` (if needed for local launch).
2.  Building the project (`mvn clean package`).
3.  Submitting the job with correct parameters.

```bash
bash run_pipeline.sh
```
