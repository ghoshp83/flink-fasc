package com.flinkfasc.aws.config;

/**
 * Minimal interface that exposes the AWS-specific configuration properties required by
 * {@link AwsConfig} to construct SDK clients and FASC service beans.
 *
 * <h3>Design rationale</h3>
 * {@code fasc-aws} must not depend on {@code fasc-coordinator} at compile time — the
 * coordinator module already depends on this module, so importing coordinator classes here
 * would create a circular dependency. By defining a narrow interface in {@code fasc-aws},
 * the coordinator's {@code FascCoordinatorProperties} class can implement it and be
 * injected into {@link AwsConfig} through the interface type.
 *
 * <h3>Wiring in the coordinator</h3>
 * In {@code fasc-coordinator}, {@code FascCoordinatorProperties} should declare:
 * <pre>
 * {@code
 * @ConfigurationProperties(prefix = "fasc")
 * public class FascCoordinatorProperties implements FascAwsProperties {
 *     // ... all other fields ...
 *     private String awsRegion;
 *     private String leaderTable = "fasc-leader";
 *     private String lockTable   = "fasc-coordinator-lock";
 *     private String savepointTable = "fasc-savepoints";
 *
 *     @Override public String getAwsRegion()      { return awsRegion; }
 *     @Override public String getLeaderTable()    { return leaderTable; }
 *     @Override public String getLockTable()      { return lockTable; }
 *     @Override public String getSavepointTable() { return savepointTable; }
 * }
 * }
 * </pre>
 *
 * <p>Spring will then satisfy the {@code FascAwsProperties} injection point in
 * {@link AwsConfig} with the {@code FascCoordinatorProperties} bean.
 */
public interface FascAwsProperties {

    /**
     * Returns the AWS region used for all SDK client construction.
     *
     * <p>Maps to configuration property {@code fasc.aws-region} (or equivalent).
     * Must be a valid AWS region identifier such as {@code "eu-west-1"} or
     * {@code "us-east-1"}.
     *
     * @return the AWS region string; must not be null or blank
     */
    String getAwsRegion();

    /**
     * Returns the name of the DynamoDB table that tracks the active Flink application.
     *
     * <p>Expected schema: PK (String) {@code "active-flink-app"}, with attributes
     * {@code appId}, {@code jobId}, {@code version}, {@code updatedAt}.
     *
     * @return DynamoDB table name; defaults to {@code "fasc-leader"}
     */
    String getLeaderTable();

    /**
     * Returns the name of the DynamoDB table used for coordinator mutual exclusion.
     *
     * <p>Expected schema: PK (String) {@code "coordinator-lock"}, with attributes
     * {@code coordinatorId}, {@code acquiredAt}, {@code ttlEpoch}, {@code version}.
     * The {@code ttlEpoch} attribute should be configured as the DynamoDB TTL attribute
     * to enable automatic expiry of stale locks.
     *
     * @return DynamoDB table name; defaults to {@code "fasc-coordinator-lock"}
     */
    String getLockTable();

    /**
     * Returns the name of the DynamoDB table used to store savepoint metadata.
     *
     * <p>Expected schema: PK (String) {@code "savepoint-latest"}, plus all savepoint
     * metadata attributes.
     *
     * @return DynamoDB table name; defaults to {@code "fasc-savepoints"}
     */
    String getSavepointTable();
}
