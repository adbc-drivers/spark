// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sparkutil

const (
	// STATEMENT OPTION KEYS

	// StatementOptionIngestStagingAreaURI specifies the staging area when
	// ingesting data.  It may also be set at the database level.
	// Depending on the driver, this can be something like an S3 URI or
	// file:/// URI.  The database may require additional configuration to
	// be able to use the staging area.  Consult your vendor's
	// documentation.
	StatementOptionIngestStagingAreaURI = "spark.ingest.staging_area_uri"

	// The LOCATION clause for Spark's CREATE TABLE. Some implementations,
	// like Amazon EMR, require this to be set to create a table.
	OptionIngestLocation = "spark.ingest.location"

	// CONNECTION OPTION KEYS

	// OptionHost specifies the host to connect to
	OptionHost = "spark.host"
	// OptionPort specifies the port to connect to
	OptionPort = "spark.port"
	// OptionApi specifies the underlying API the driver will use to talk to Spark
	OptionApi = "spark.api"
	// OptionAuthType specifies the authentication method used by the driver
	OptionAuthType = "spark.auth_type"
	// OptionSchema specifies the default schema to connect to
	OptionSchema                    = "spark.schema"
	OptionUseTls                    = "spark.tls"
	OptionValidateServerCertificate = "spark.validate_server_certificate"

	// Spark Configuration Prefix
	// Options starting with this prefix are passed to the Spark session configuration
	// Example: spark.opt.executor.memory=4g -> spark.executor.memory=4g
	OptionSparkConfigPrefix = "spark.opt."

	// Ingest options

	// OptionIngestS3BaseEndpoint specifies a custom S3 endpoint URL for
	// ingestion (e.g., for MinIO or other S3-compatible stores).
	// May be set at the database level.
	OptionIngestS3BaseEndpoint = "spark.ingest.s3.base_endpoint"

	// OptionIngestS3UsePathStyle controls whether S3 path-style addressing
	// is used (as opposed to virtual-hosted-style).  Set to "true" for
	// S3-compatible stores like MinIO.  May be set at the database level.
	OptionIngestS3UsePathStyle = "spark.ingest.s3.use_path_style"

	// Kerberos-specific options

	// OptionKerberosServiceName specifies the kerberos service name when
	// using KERBEROS SASL auth
	OptionKerberosServiceName = "spark.kerberos.service_name"

	// Livy-specific options

	// OptionLivySessionKind specifies the Livy session type
	// Default: spark
	OptionLivySessionKind = "spark.livy.session_kind"

	// OptionLivyTimeout specifies the HTTP request timeout in seconds
	OptionLivyTimeout = "spark.livy.timeout"

	OptionLivyBaseURL = "spark.livy.base_url"

	// OptionLivySessionTTL specifies the session time-to-live (e.g., "2h", "30m")
	// Available in EMR 7.8.0+
	OptionLivySessionTTL = "spark.livy.session_ttl"

	// OptionHeartbeatTimeout specifies the Livy session heartbeat timeout in seconds
	OptionLivyHeartbeatTimeout = "spark.livy.heartbeat_timeout"

	// OptionLivySessionId specifies the Livy session ID to connect to an existing session
	OptionLivySessionId = "spark.livy.session_id"

	OptionLivyReleaseSession = "spark.livy.release_session"

	// Basic Authentication Options (when auth_type=basic)
	// These use the standard ADBC `username` and `password`

	// AWS Authentication Options (when auth_type=aws_sigv4)

	// OptionAWSRegion specifies the AWS region (required for aws_sigv4)
	OptionLivyAWSRegion = "spark.livy.aws.region"

	// OptionLivyAWSProfile specifies the AWS profile name
	OptionLivyAWSProfile = "spark.livy.aws.profile"

	// OptionLivyAWSAccessKeyID specifies explicit AWS access key
	OptionLivyAWSAccessKeyID = "spark.livy.aws.access_key_id"

	// OptionLivyAWSSecretAccessKey specifies explicit AWS secret key
	OptionLivyAWSSecretAccessKey = "spark.livy.aws.secret_access_key"

	// OptionLivyAWSSessionToken specifies AWS session token for temporary credentials
	OptionLivyAWSSessionToken = "spark.livy.aws.session_token"

	// EMR Serverless Options

	// OptionLivyAWSEMRExecutionRoleArn specifies the AWS EMR Serverless execution role ARN
	// This is required when connecting to AWS EMR Serverless
	OptionLivyAWSExecutionRoleArn = "spark.livy.aws.emr_serverless.execution_role_arn"

	// Azure Authentication Options (when auth_type=azure_token)

	// OptionLivyAzureCredential selects how the Microsoft Entra ID token is
	// obtained. See the OptionValueAzureCredential* constants for supported
	// values. Defaults to "default" (the azidentity DefaultAzureCredential
	// chain: environment variables, workload identity, managed identity,
	// Azure CLI, ...).
	OptionLivyAzureCredential = "spark.livy.azure.credential"

	// OptionLivyAzureTenantID specifies the Entra ID tenant (directory) ID.
	// Required for the "client_secret" credential; optional otherwise.
	OptionLivyAzureTenantID = "spark.livy.azure.tenant_id"

	// OptionLivyAzureClientID specifies the Entra ID application (client) ID.
	// Required for the "client_secret" credential; selects the user-assigned
	// identity for "managed_identity".
	OptionLivyAzureClientID = "spark.livy.azure.client_id"

	// OptionLivyAzureClientSecret specifies the client secret for the
	// "client_secret" credential.
	OptionLivyAzureClientSecret = "spark.livy.azure.client_secret"

	// OptionLivyAzureTokenScope overrides the OAuth2 scope requested for the
	// bearer token. When unset, the scope is inferred from the endpoint host:
	// "https://api.fabric.microsoft.com/.default" for Microsoft Fabric hosts,
	// "https://dev.azuresynapse.net/" otherwise (Azure Synapse).
	OptionLivyAzureTokenScope = "spark.livy.azure.token_scope"

	// Spark Connect-specific options

	// OptionConnectSessionId specifies the Spark Connect session ID to reuse
	OptionConnectSessionId = "spark.connect.session_id"

	// OptionConnectReleaseSession specifies whether to release the Spark Connect session when closed
	OptionConnectReleaseSession = "spark.connect.release_session"

	// OPTION VALUES

	// OptionApi

	OptionValueApiThriftBinary = "thrift+binary"
	OptionValueApiThriftHttp   = "thrift+http"
	OptionValueApiLivy         = "livy"
	OptionValueApiConnect      = "connect"
	// TODO: EMR StartJob API

	// OptionAuthType

	// Spark Thrift auth types

	OptionValueAuthTypeNone     = "none"
	OptionValueAuthTypeKerberos = "kerberos"
	OptionValueAuthTypeLdap     = "ldap"
	OptionValueAuthTypeNoSasl   = "nosasl"
	OptionValueAuthTypePlain    = "plain"

	// Spark Livy auth types

	OptionValueAuthTypeBasic      = "basic"
	OptionValueAuthTypeAwsSigv4   = "aws_sigv4"
	OptionValueAuthTypeAzureToken = "azure_token"

	// OptionLivyAzureCredential values

	OptionValueAzureCredentialDefault         = "default"
	OptionValueAzureCredentialCli             = "cli"
	OptionValueAzureCredentialClientSecret    = "client_secret"
	OptionValueAzureCredentialEnvironment     = "environment"
	OptionValueAzureCredentialManagedIdentity = "managed_identity"

	// Spark Connect auth types

	OptionValueAuthTypeToken = "token"

	// OptionLivySessionKind

	// Livy-specific values
	OptionValueSessionKindSql     = "sql"
	OptionValueSessionKindSpark   = "spark"
	OptionValueSessionKindPySpark = "pyspark"
)
