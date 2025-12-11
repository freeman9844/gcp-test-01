#!/bin/bash

# Configuration - PLEASE UPDATE THESE VALUES
PROJECT_ID="jwlee-argolis-202104"
REGION="asia-northeast3"
INPUT_TABLE="${PROJECT_ID}:KR_Dataset002.customer_pii_raw"
OUTPUT_TABLE="${PROJECT_ID}:KR_Dataset002.customer_pii_raw_out"
# Optional: DEIDENTIFY_TEMPLATE="projects/..." 

# Build the project
echo "Building project..."
mvn clean package -DskipTests

# Set JAVA_HOME explicitly since system wrapper is failing
# Attempt to find Homebrew OpenJDK 25
if [ -d "/opt/homebrew/opt/openjdk@25" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk@25"
elif [ -d "/opt/homebrew/Cellar/openjdk/25.0.1" ]; then
    export JAVA_HOME="/opt/homebrew/Cellar/openjdk/25.0.1"
fi

# Fallback to PATH if not found, but prefer JAVA_HOME/bin/java
if [ -n "$JAVA_HOME" ]; then
    JAVA_CMD="$JAVA_HOME/bin/java"
else
    JAVA_CMD="java"
fi

echo "Using Java: $JAVA_CMD"

# Run the pipeline
echo "Running Dataflow pipeline..."
$JAVA_CMD -cp target/dataflow-dlp-pipeline-1.0-SNAPSHOT.jar com.example.dataflow.DlpPipeline \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=$REGION \
  --inputTable=$INPUT_TABLE \
  --outputTable=$OUTPUT_TABLE \
  --batchSize=1000 \
  --gcpTempLocation=gs://$PROJECT_ID-dataflow/temp \
  --stagingLocation=gs://$PROJECT_ID-dataflow/staging \
  --network=jwlee-vpc-001 \
  --subnetwork=regions/$REGION/subnetworks/jwlee-vpc-001

# Note: Add --deidentifyTemplateName=$DEIDENTIFY_TEMPLATE if you want to use a specific template instead of default RRN masking.
