// Copyright (c) 2026 ADBC Drivers Contributors
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

package livyimpl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/adbc-drivers/spark/go/sparkutil"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type AuthType uint8
type SessionKind string

const (
	AuthTypeNone AuthType = iota
	AuthTypeBasic
	AuthTypeAwsSigV4
	AuthTypeAzureToken
)

const (
	SessionKindSql     SessionKind = "sql"
	SessionKindSpark   SessionKind = "spark"
	SessionKindPySpark SessionKind = "pyspark"
)

type ConnectionOpts struct {
	SessionKind SessionKind
	AuthType    AuthType

	BaseURL                   string
	HttpTimeoutSeconds        uint
	HeartbeatTimeoutSeconds   uint
	QueryTimeoutSeconds       uint
	Username                  string
	Password                  string
	SessionTtl                string
	ExistingSessionId         *int64
	DeleteSessionOnClose      bool
	ValidateServerCertificate bool

	AwsConfig aws.Config
}

// livyClient handles communication with the Livy REST API
type livyClient struct {
	sessionID int64
	catalog   string //nolint:unused
	schema    string //nolint:unused

	sessionConfig        map[string]string
	sessionTtl           string
	httpClient           *http.Client
	heartbeatTimeout     time.Duration
	queryTimeout         time.Duration
	baseURL              string
	sessionKind          SessionKind
	authType             AuthType
	awsConfig            aws.Config
	username             string
	password             string
	deleteSessionOnClose bool
	azCred               azcore.TokenCredential
}

// NewClient creates a new SparkClient over Livy client
func NewClient(ctx context.Context, opts ConnectionOpts, sessionConfig map[string]string) (sparkbase.SparkClient, error) {
	httpClient := &http.Client{
		Timeout: time.Duration(float64(opts.HttpTimeoutSeconds) * float64(time.Second)),
	}
	if !opts.ValidateServerCertificate {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	var azCred azcore.TokenCredential
	if opts.AuthType == AuthTypeAzureToken {
		// TODO: add option (like MSSQL) to use various credential types
		var err error
		options := &azidentity.AzureCLICredentialOptions{}
		azCred, err = azidentity.NewAzureCLICredential(options)
		if err != nil {
			return nil, err
		}
	}

	client := &livyClient{
		sessionID:            -1,
		sessionConfig:        sessionConfig,
		baseURL:              opts.BaseURL,
		httpClient:           httpClient,
		queryTimeout:         time.Duration(float64(opts.QueryTimeoutSeconds) * float64(time.Second)),
		heartbeatTimeout:     time.Duration(float64(opts.HeartbeatTimeoutSeconds) * float64(time.Second)),
		authType:             opts.AuthType,
		sessionKind:          opts.SessionKind,
		sessionTtl:           opts.SessionTtl,
		awsConfig:            opts.AwsConfig,
		username:             opts.Username,
		password:             opts.Password,
		deleteSessionOnClose: opts.DeleteSessionOnClose,
		azCred:               azCred,
	}

	err := client.openSession(ctx, opts.ExistingSessionId)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (c *livyClient) BackendName() string {
	return "Apache Livy"
}

// Session represents a Livy session
type Session struct {
	ID                  int64          `json:"id"`
	AppID               string         `json:"appId"`
	Owner               string         `json:"owner"`
	ProxyUser           string         `json:"proxyUser"`
	Kind                string         `json:"kind"`
	Log                 []string       `json:"log"`
	State               string         `json:"state"`
	AppInfo             map[string]any `json:"appInfo"`
	HeartbeatTimeoutSec int            `json:"heartbeatTimeoutInSecond,omitempty"`
	TTL                 string         `json:"ttl,omitempty"`
}

// SessionState represents possible session states
type SessionState string

const (
	SessionStateNotStarted   SessionState = "not_started"
	SessionStateStarting     SessionState = "starting"
	SessionStateIdle         SessionState = "idle"
	SessionStateBusy         SessionState = "busy"
	SessionStateShuttingDown SessionState = "shutting_down"
	SessionStateError        SessionState = "error"
	SessionStateDead         SessionState = "dead"
	SessionStateKilled       SessionState = "killed"
	SessionStateSuccess      SessionState = "success"
)

// Statement represents a Livy statement
type Statement struct {
	ID        int              `json:"id"`
	Code      string           `json:"code"`
	State     string           `json:"state"`
	Output    *StatementOutput `json:"output"`
	Progress  float64          `json:"progress"`
	Started   int64            `json:"started"`
	Completed int64            `json:"completed"`
}

// StatementOutput represents statement output
type StatementOutput struct {
	Status         string         `json:"status"`
	ExecutionCount int            `json:"execution_count"`
	Data           map[string]any `json:"data"`
	Ename          string         `json:"ename"`
	Evalue         string         `json:"evalue"`
	Traceback      []string       `json:"traceback"`
}

// StatementState represents possible statement states
type StatementState string

const (
	StatementStateWaiting    StatementState = "waiting"
	StatementStateRunning    StatementState = "running"
	StatementStateAvailable  StatementState = "available"
	StatementStateError      StatementState = "error"
	StatementStateCancelling StatementState = "cancelling"
	StatementStateCancelled  StatementState = "cancelled"
)

// CreateSessionRequest represents a session creation request
type CreateSessionRequest struct {
	Kind                string            `json:"kind"`
	ProxyUser           string            `json:"proxyUser,omitempty"`
	Jars                []string          `json:"jars,omitempty"`
	PyFiles             []string          `json:"pyFiles,omitempty"`
	Files               []string          `json:"files,omitempty"`
	DriverMemory        string            `json:"driverMemory,omitempty"`
	DriverCores         int               `json:"driverCores,omitempty"`
	ExecutorMemory      string            `json:"executorMemory,omitempty"`
	ExecutorCores       int               `json:"executorCores,omitempty"`
	NumExecutors        int               `json:"numExecutors,omitempty"`
	Archives            []string          `json:"archives,omitempty"`
	Queue               string            `json:"queue,omitempty"`
	Name                string            `json:"name,omitempty"`
	Conf                map[string]string `json:"conf,omitempty"`
	HeartbeatTimeoutSec int               `json:"heartbeatTimeoutInSecond,omitempty"`
	TTL                 string            `json:"ttl,omitempty"`
}

// CreateStatementRequest represents a statement execution request
type CreateStatementRequest struct {
	Code string `json:"code"`
	Kind string `json:"kind,omitempty"`
}

func (c *livyClient) openSession(ctx context.Context, existingSessionId *int64) error {
	if existingSessionId != nil {
		c.sessionID = *existingSessionId
		// Wait for session to be ready (TODO: configurable timeout)
		timeout := time.Duration(5 * 60 * float64(time.Second))
		if err := c.WaitForSessionReady(ctx, timeout); err != nil {
			// TODO: try to list existing sessions?
			return adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("[spark] session %d failed to start: %v", *existingSessionId, err),
			}
		}
		return nil
	}

	// Create session request
	req := CreateSessionRequest{
		Kind:         string(c.sessionKind),
		Conf:         c.sessionConfig,
		DriverMemory: "1g",
		DriverCores:  1,
	}

	if v, ok := c.sessionConfig["spark.executor.cores"]; ok {
		intv, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "spark.executor.cores must be integer",
			}
		}
		req.ExecutorCores = int(intv)
	}
	if v, ok := c.sessionConfig["spark.executor.memory"]; ok {
		req.ExecutorMemory = v
	}

	if v, ok := c.sessionConfig["spark.driver.cores"]; ok {
		intv, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "spark.driver.cores must be integer",
			}
		}
		req.DriverCores = int(intv)
	}
	if v, ok := c.sessionConfig["spark.driver.memory"]; ok {
		req.DriverMemory = v
	}

	heartbeatTimeoutSec := c.heartbeatTimeout.Seconds()
	if heartbeatTimeoutSec > 0 {
		req.HeartbeatTimeoutSec = int(heartbeatTimeoutSec)
	}

	if c.sessionTtl != "" {
		req.TTL = c.sessionTtl
	}

	// Create session
	session, err := c.CreateSession(ctx, req)
	if err != nil {
		return err
	}
	c.sessionID = session.ID

	// Wait for session to be ready (TODO: configurable timeout)
	timeout := time.Duration(5 * 60 * float64(time.Second))
	if err := c.WaitForSessionReady(ctx, timeout); err != nil {
		_ = c.DeleteSession(ctx)
		return adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("session failed to start: %v", err),
		}
	}

	return nil
}

// CreateSession creates a new Livy session
func (c *livyClient) CreateSession(ctx context.Context, req CreateSessionRequest) (*Session, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[spark] failed to serialize CreateSesssionRequest: %v", err),
		}
	}

	resp, err := c.doRequest(ctx, "POST", "/sessions", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	// TODO: don't swallow error
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusBadRequest && strings.Contains(c.baseURL, "amazonaws.com") {
			_, ok := c.sessionConfig["emr-serverless.session.executionRoleArn"]
			if !ok {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("failed to create session: (%d) %s. If using AWS EMR Serverless, you must set 'spark.opt.emr-serverless.session.executionRoleArn'", resp.StatusCode, string(body)),
				}
			}
		}

		return nil, adbc.Error{
			Code: sparkutil.HttpStatusToCode(resp.StatusCode),
			Msg:  fmt.Sprintf("[spark] failed to create session: (%d) %s", resp.StatusCode, string(body)),
		}
	}

	var session Session
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[spark] failed to decode CreateSesssion response: %v", err),
		}
	}
	return &session, nil
}

// GetSession retrieves session information
func (c *livyClient) GetSession(ctx context.Context, sessionID int64) (*Session, error) {
	url := fmt.Sprintf("/sessions/%d", sessionID)
	resp, err := c.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	// TODO: don't swallow error
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get session: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var session Session
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		return nil, fmt.Errorf("failed to decode GetSesssion response: %w", err)
	}

	return &session, nil
}

// DeleteSession deletes a session
func (c *livyClient) DeleteSession(ctx context.Context) error {
	url := fmt.Sprintf("/sessions/%d", c.sessionID)
	resp, err := c.doRequest(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}
	// TODO: don't swallow error
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete session: status=%d, body=%s", resp.StatusCode, string(body))
	}

	c.sessionID = -1

	return nil
}

func (c *livyClient) Close(ctx context.Context) error {
	if c.deleteSessionOnClose {
		url := fmt.Sprintf("/sessions/%d", c.sessionID)
		resp, err := c.doRequest(ctx, "DELETE", url, nil)
		if err != nil {
			return err
		}
		// TODO: don't swallow error
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to delete session: status=%d, body=%s", resp.StatusCode, string(body))
		}
	}

	return nil
}

// WaitForSessionReady waits for the session to be in idle state
func (c *livyClient) WaitForSessionReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if timeout > 0 && time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for session to be ready")
			}

			session, err := c.GetSession(ctx, c.sessionID)
			if err != nil {
				return fmt.Errorf("failed to get session status: %w", err)
			}

			switch SessionState(session.State) {
			case SessionStateIdle:
				return nil
			case SessionStateError, SessionStateDead, SessionStateKilled:
				return fmt.Errorf("session failed with state: %s", session.State)
			case SessionStateStarting, SessionStateNotStarted:
				// Continue waiting
				continue
			default:
				return fmt.Errorf("unexpected session state: %s", session.State)
			}
		}
	}
}

// CreateStatement executes a statement in a session
func (c *livyClient) CreateStatement(ctx context.Context, req CreateStatementRequest) (*Statement, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal statement request: %w", err)
	}

	url := fmt.Sprintf("/sessions/%d/statements", c.sessionID)
	resp, err := c.doRequest(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	// TODO: don't swallow error
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to create statement: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var stmt Statement
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(&stmt); err != nil {
		return nil, fmt.Errorf("failed to decode statement response: %w", err)
	}

	return &stmt, nil
}

// GetStatement retrieves statement information
func (c *livyClient) GetStatement(ctx context.Context, sessionID int64, statementID int) (*Statement, error) {
	url := fmt.Sprintf("/sessions/%d/statements/%d", sessionID, statementID)
	resp, err := c.doRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	// TODO: don't swallow error
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get statement: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var stmt Statement
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(&stmt); err != nil {
		return nil, fmt.Errorf("failed to decode statement response: %w", err)
	}

	return &stmt, nil
}

// WaitForStatementComplete waits for a statement to complete
func (c *livyClient) WaitForStatementComplete(ctx context.Context, statementID int, timeout time.Duration) (*Statement, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if timeout > 0 && time.Now().After(deadline) {
				return nil, fmt.Errorf("timeout waiting for statement to complete")
			}

			stmt, err := c.GetStatement(ctx, c.sessionID, statementID)
			if err != nil {
				return nil, fmt.Errorf("failed to get statement status: %w", err)
			}

			switch StatementState(stmt.State) {
			case StatementStateAvailable, StatementStateError:
				return stmt, nil
			case StatementStateCancelled:
				return nil, fmt.Errorf("statement was cancelled")
			case StatementStateWaiting, StatementStateRunning:
				// Continue waiting
				continue
			default:
				return nil, fmt.Errorf("unexpected statement state: %s", stmt.State)
			}
		}
	}
}

// doRequest performs an HTTP request with appropriate authentication
func (c *livyClient) doRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	url := c.baseURL + path

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Apply authentication
	switch c.authType {
	case AuthTypeAwsSigV4:
		if err := c.signRequestWithSigV4(ctx, req); err != nil {
			return nil, fmt.Errorf("failed to sign request: %w", err)
		}
	case AuthTypeAzureToken:
		options := policy.TokenRequestOptions{
			Scopes: []string{"https://dev.azuresynapse.net/"},
		}
		token, err := c.azCred.GetToken(ctx, options)
		if err != nil {
			return nil, fmt.Errorf("failed to get Azure token: %w", err)
		}
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token.Token))
	case AuthTypeBasic:
		req.SetBasicAuth(c.username, c.password)
	case AuthTypeNone:
		// No authentication
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	return resp, nil
}

// signRequestWithSigV4 signs an HTTP request using AWS SigV4
func (c *livyClient) signRequestWithSigV4(ctx context.Context, req *http.Request) error {
	// Get credentials
	creds, err := c.awsConfig.Credentials.Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("failed to retrieve AWS credentials: %w", err)
	}

	// Create signer
	signer := v4.NewSigner()

	// Read body if present (for signing)
	var bodyBytes []byte
	if req.Body != nil {
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return fmt.Errorf("failed to read request body: %w", err)
		}
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	// Compute payload hash
	hash := sha256.Sum256(bodyBytes)
	payloadHash := hex.EncodeToString(hash[:])

	// Sign the request
	// Service name for EMR Serverless Livy is "emr-serverless"
	err = signer.SignHTTP(ctx, creds, req, payloadHash, "emr-serverless", c.awsConfig.Region, time.Now())
	if err != nil {
		return fmt.Errorf("failed to sign request with SigV4: %w", err)
	}

	// Restore body
	if bodyBytes != nil {
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	return nil
}

func (c *livyClient) VendorVersion(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultVendorVersionImpl(c, ctx, mem)
}

func (c *livyClient) CurrentCatalog(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultCurrentCatalogImpl(c, ctx, mem)
}

func (c *livyClient) SetCurrentCatalog(ctx context.Context, mem memory.Allocator, catalog string) error {
	return sparkbase.DefaultSetCurrentCatalogImpl(c, ctx, mem, catalog)
}

func (c *livyClient) CurrentSchema(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultCurrentSchemaImpl(c, ctx, mem)
}

func (c *livyClient) SetCurrentSchema(ctx context.Context, mem memory.Allocator, schema string) error {
	return sparkbase.DefaultSetCurrentSchemaImpl(c, ctx, mem, schema)
}

func (c *livyClient) ExecuteQuery(ctx context.Context, query sparkbase.QueryContext) (array.RecordReader, int64, error) {
	// TODO(serramatutu): do we need this really?
	// Check if we're using SQL session kind
	if c.sessionKind != SessionKindSql {
		return nil, -1, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  "schema retrieval not supported for Spark/PySpark sessions",
		}
	}

	stmt, err := c.CreateStatement(ctx, CreateStatementRequest{Code: query.Query})
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("failed to execute query: %v", err),
		}
	}

	// Wait for data statement to complete
	stmt, err = c.WaitForStatementComplete(ctx, stmt.ID, c.queryTimeout)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("query execution failed: %v", err),
		}
	}

	// Check for errors
	if stmt.Output == nil {
		if StatementState(stmt.State) == StatementStateError {
			return nil, -1, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "[spark] livy: unknown error",
			}
		}
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "[spark] livy: no output from statement",
		}
	} else if stmt.Output.Status == "error" {
		return nil, -1, adbcErrFromLivyOutput(adbc.StatusUnknown, stmt.Output, "query error")
	}

	// Step 2: Get schema
	var schema *arrow.Schema
	schema, err = parseSchemaFromSQLResult(stmt)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to parse schema: %v", err),
		}
	}

	// Parse data
	rows, err := parseDataFromSQLResult(stmt, schema)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to parse SQL result data: %v", err),
		}
	}

	// Create a record reader from the parsed rows
	reader, err := newJSONRecordReader(query.Mem, schema, rows)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to create reader: %v", err),
		}
	}

	return reader, int64(len(rows)), nil
}

func (c *livyClient) ExecuteUpdate(ctx context.Context, query sparkbase.QueryContext) (int64, error) {
	stmt, err := c.CreateStatement(ctx, CreateStatementRequest{Code: query.Query})
	if err != nil {
		return -1, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("failed to execute query: %v", err),
		}
	}
	stmt, err = c.WaitForStatementComplete(ctx, stmt.ID, c.queryTimeout)
	if err != nil {
		return -1, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("query execution failed: %v", err),
		}
	}
	// Check for errors
	if stmt.Output.Status == "error" {
		return -1, adbcErrFromLivyOutput(adbc.StatusUnknown, stmt.Output, "query error")
	}
	return -1, nil
}

func (c *livyClient) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	return sparkbase.DefaultGetCatalogsImpl(c, ctx, catalogFilter)
}

func (c *livyClient) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	return sparkbase.DefaultGetDBSchemasForCatalogImpl(c, ctx, catalog, schemaFilter)
}

func (c *livyClient) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	return sparkbase.DefaultGetTablesForDBSchemaImpl(c, ctx, catalog, schema, tableFilter, columnFilter, includeColumns)
}

func (c *livyClient) GetOption(_ context.Context, key string) (string, bool, error) {
	switch key {
	case sparkutil.OptionLivySessionId:
		return strconv.FormatInt(c.sessionID, 10), true, nil
	default:
		return "", false, nil
	}
}

func (c *livyClient) GetOptionInt(_ context.Context, key string) (int64, bool, error) {
	switch key {
	case sparkutil.OptionLivySessionId:
		return c.sessionID, true, nil
	default:
		return 0, false, nil
	}
}

// parseSchemaFromSQLResult extracts schema from SQL session result
func parseSchemaFromSQLResult(stmt *Statement) (*arrow.Schema, error) {
	// SQL session results come in application/json with schema metadata
	if jsonData, ok := stmt.Output.Data["application/json"]; ok {
		dataMap, ok := jsonData.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unexpected SQL result format")
		}

		// Check if schema is embedded in the response
		if schemaData, ok := dataMap["schema"]; ok {
			schemaBytes, err := json.Marshal(schemaData)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal schema: %w", err)
			}
			return parseSparkSchemaJSON(string(schemaBytes))
		}
	}
	return nil, fmt.Errorf("unable to extract schema from SQL result")
}

// parseDataFromSQLResult extracts data rows from SQL session result
func parseDataFromSQLResult(stmt *Statement, schema *arrow.Schema) ([]map[string]any, error) {
	// SQL session results come in application/json format
	if jsonData, ok := stmt.Output.Data["application/json"]; ok {
		dataMap, ok := jsonData.(map[string]any)
		if ok {
			if dataArray, ok := dataMap["data"].([]any); ok {
				var rows []map[string]any
				for _, row := range dataArray {
					rowMap, err := rowToMap(row, schema)
					if err == nil {
						rows = append(rows, rowMap)
					}
				}
				return rows, nil
			}
		}
	}

	// Fallback: parse text/plain table output
	if textData, ok := stmt.Output.Data["text/plain"].(string); ok {
		return parseTableOutput(textData, schema)
	}

	return nil, fmt.Errorf("unable to extract data from SQL result")
}

// parseTableOutput converts SQL table text output to row maps
func parseTableOutput(output string, schema *arrow.Schema) ([]map[string]any, error) {
	var rows []map[string]any
	lines := strings.Split(output, "\n")

	dataStarted := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "+") {
			continue
		}

		if strings.HasPrefix(line, "|") {
			if !dataStarted {
				// Skip header row
				dataStarted = true
				continue
			}

			// Parse data row
			parts := strings.Split(line, "|")
			rowData := make(map[string]any)
			fieldIdx := 0
			for _, part := range parts {
				val := strings.TrimSpace(part)
				if val != "" && fieldIdx < schema.NumFields() {
					rowData[schema.Field(fieldIdx).Name] = val
					fieldIdx++
				}
			}

			if len(rowData) > 0 {
				rows = append(rows, rowData)
			}
		}
	}

	return rows, nil
}

// rowToMap converts a row from SQL result to a map
func rowToMap(row any, schema *arrow.Schema) (map[string]any, error) {
	if rowMap, ok := row.(map[string]any); ok {
		return rowMap, nil
	}

	// If row is an array, map it to schema fields
	if rowArray, ok := row.([]any); ok {
		rowData := make(map[string]any)
		for i, val := range rowArray {
			if i < schema.NumFields() {
				rowData[schema.Field(i).Name] = val
			}
		}
		return rowData, nil
	}

	return nil, fmt.Errorf("unsupported row format: %T", row)
}

var _ sparkbase.SparkClient = (*livyClient)(nil)

func adbcErrFromLivyOutput(defaultStatus adbc.Status, output *StatementOutput, context string, contextArgs ...any) error {
	var msg strings.Builder
	msg.WriteString("[spark] livy: ")
	fmt.Fprintf(&msg, context, contextArgs...)
	msg.WriteString(": ")
	msg.WriteString(output.Ename)
	msg.WriteString(": ")
	msg.WriteString(output.Evalue)

	err := adbc.Error{
		Code: defaultStatus,
		Msg:  msg.String(),
	}

	// Guess from output.Evalue because Livy doesn't break out the error code
	switch {
	// N.B. there are multiple messages with this prefix
	case strings.HasPrefix(output.Evalue, "[INSERT_COLUMN_ARITY_MISMATCH"):
		err.Code = adbc.StatusAlreadyExists
		err.SqlState = [5]byte{'2', '1', 'S', '0', '1'}
	case strings.HasPrefix(output.Evalue, "[TABLE_OR_VIEW_NOT_FOUND]"):
		err.Code = adbc.StatusNotFound
		err.SqlState = [5]byte{'4', '2', 'P', '0', '1'}
	case strings.HasPrefix(output.Evalue, "[TABLE_OR_VIEW_ALREADY_EXISTS]"):
		err.Code = adbc.StatusAlreadyExists
		err.SqlState = [5]byte{'4', '2', 'P', '0', '7'}
	}
	return err
}
