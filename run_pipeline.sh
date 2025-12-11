#!/bin/bash

# ==============================================================================
# Dataflow DLP Pipeline Execution Script
# ==============================================================================
# Purpose: Builds the project (Fat JAR) and submits the Dataflow job.
# Usage: ./run_pipeline.sh
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Configuration
# ------------------------------------------------------------------------------
# Google Cloud Project ID
PROJECT_ID="jwlee-argolis-202104"
# Dataflow Region
REGION="asia-northeast3"

# BigQuery Input/Output Tables (Project:Dataset.Table)
INPUT_TABLE="${PROJECT_ID}:KR_Dataset002.customer_pii_raw"
OUTPUT_TABLE="${PROJECT_ID}:KR_Dataset002.customer_pii_raw_out"

# Network Configuration (Required for private IPs or specific VPCs)
NETWORK="jwlee-vpc-001"
SUBNETWORK="regions/$REGION/subnetworks/jwlee-vpc-001"

# Temporary and Staging Buckets for Dataflow
TEMP_LOCATION="gs://$PROJECT_ID-dataflow/temp"
STAGING_LOCATION="gs://$PROJECT_ID-dataflow/staging"

# ------------------------------------------------------------------------------
# 2. Build Project
# ------------------------------------------------------------------------------
echo "Building project with Maven..."
# clean package: Rebuilds the artifact
# -DskipTests: Skips unit tests for faster deployment (optional, remove to run tests)
mvn clean package -DskipTests

# ------------------------------------------------------------------------------
# 3. Java Environment Setup
# ------------------------------------------------------------------------------
# Ensure we use a compatible Java version (Dataflow workers support Java 11/17/21)
# If JAVA_HOME is set, use it. Otherwise, look for specific Homebrew paths or default java.
if [ -z "$JAVA_HOME" ]; then
    if [ -d "/opt/homebrew/opt/openjdk@21" ]; then
        export JAVA_HOME="/opt/homebrew/opt/openjdk@21"
    elif [ -d "/opt/homebrew/opt/openjdk@25" ]; then
        # Fallback to newer JDK if available (local runner can handle it)
        export JAVA_HOME="/opt/homebrew/opt/openjdk@25"
    fi
fi

if [ -n "$JAVA_HOME" ]; then
    JAVA_CMD="$JAVA_HOME/bin/java"
else
    JAVA_CMD="java"
fi

echo "Using Java: $JAVA_CMD"

# ------------------------------------------------------------------------------
# 4. Run Pipeline
# ------------------------------------------------------------------------------
echo "Submitting Dataflow pipeline..."

$JAVA_CMD -cp target/dataflow-dlp-pipeline-1.0-SNAPSHOT.jar com.example.dataflow.DlpPipeline \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=$REGION \
  --inputTable=$INPUT_TABLE \
  --outputTable=$OUTPUT_TABLE \
  --batchSize=1000 \
  --gcpTempLocation=$TEMP_LOCATION \
  --stagingLocation=$STAGING_LOCATION \
  --network=$NETWORK \
  --subnetwork=$SUBNETWORK

# Note: Add --deidentifyTemplateName="projects/..." if using a DLP Template.
