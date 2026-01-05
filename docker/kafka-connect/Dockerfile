# ============================================
# Kafka Connect with Debezium Oracle Connector
# ============================================
# Custom image that includes:
# - Debezium Oracle connector
# - Oracle JDBC driver (ojdbc11)
# - Oracle Instant Client libraries

FROM debezium/connect:3.0.0.Final

LABEL maintainer="o2kafka"
LABEL description="Kafka Connect with Debezium Oracle connector and drivers"

# Switch to root for installations
USER root

# Install required packages
RUN microdnf install -y \
    libaio \
    unzip \
    && microdnf clean all

# Create directory for Oracle Instant Client
RUN mkdir -p /opt/oracle

# ============================================
# Oracle Instant Client Installation
# ============================================
# Download Oracle Instant Client Basic and SDK
# Note: These URLs are for Oracle Instant Client 21.x
ARG ORACLE_INSTANT_CLIENT_VERSION=21.13.0.0.0
ARG ORACLE_INSTANT_CLIENT_BASE_URL=https://download.oracle.com/otn_software/linux/instantclient/2113000

# Download and extract Instant Client Basic
ADD ${ORACLE_INSTANT_CLIENT_BASE_URL}/instantclient-basic-linux.x64-${ORACLE_INSTANT_CLIENT_VERSION}dbru.zip /tmp/
RUN unzip /tmp/instantclient-basic-linux.x64-${ORACLE_INSTANT_CLIENT_VERSION}dbru.zip -d /opt/oracle/ \
    && rm /tmp/instantclient-basic-linux.x64-${ORACLE_INSTANT_CLIENT_VERSION}dbru.zip

# Set Oracle environment variables
ENV ORACLE_HOME=/opt/oracle/instantclient_21_13
ENV LD_LIBRARY_PATH=${ORACLE_HOME}:${LD_LIBRARY_PATH}
ENV PATH=${ORACLE_HOME}:${PATH}

# Create symbolic links for Oracle libraries
RUN ln -sf ${ORACLE_HOME}/libclntsh.so.21.1 ${ORACLE_HOME}/libclntsh.so \
    && ln -sf ${ORACLE_HOME}/libocci.so.21.1 ${ORACLE_HOME}/libocci.so

# ============================================
# Oracle JDBC Driver
# ============================================
# Download Oracle JDBC driver (ojdbc11)
ARG OJDBC_VERSION=21.13.0.0
ADD https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/${OJDBC_VERSION}/ojdbc11-${OJDBC_VERSION}.jar /kafka/connect/debezium-connector-oracle/

# ============================================
# XStream API (optional, for XStream mode)
# ============================================
# Note: XStream API jar must be obtained from Oracle installation
# If using LogMiner mode (default), this is not required

# ============================================
# Additional Kafka Connect plugins
# ============================================
# Add any additional transformations or converters here

# Set proper permissions
RUN chown -R kafka:kafka /opt/oracle \
    && chmod -R 755 /opt/oracle \
    && chown kafka:kafka /kafka/connect/debezium-connector-oracle/*.jar

# Configure library path for Kafka Connect
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_13:$LD_LIBRARY_PATH

# Switch back to kafka user
USER kafka

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8083/connectors || exit 1

# Default command (inherited from base image)
