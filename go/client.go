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

package spark

import (
	"context"
	"fmt"
	"github.com/adbc-drivers/spark/go/internal/connectimpl"
	"github.com/adbc-drivers/spark/go/internal/livyimpl"
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/adbc-drivers/spark/go/internal/thriftimpl"
	"github.com/adbc-drivers/spark/go/sparkutil"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"net/url"
	"strconv"
	"strings"
)

// Parse named options from an URI. Does not validate whether those options make sense or belong together.
func parseOptionsFromUri(uri *url.URL, options map[string]string) error {
	split := strings.Split(uri.Host, ":")
	switch len(split) {
	case 1:
		options[sparkutil.OptionHost] = split[0]
	case 2:
		options[sparkutil.OptionHost] = split[0]
		options[sparkutil.OptionPort] = split[1]
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[spark] invalid URI host:port",
		}
	}

	if uri.User != nil {
		if uri.User.Username() != "" {
			options[adbc.OptionKeyUsername] = uri.User.Username()
		}
		if password, ok := uri.User.Password(); ok && password != "" {
			options[adbc.OptionKeyPassword] = password
		}
	}

	queryValues, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return sparkbase.ErrToAdbcErr(adbc.StatusInvalidArgument, err, "parse URI query")
	}

	for key, values := range queryValues {
		if key == "validateservercertificate" {
			key = "validate_server_certificate"
		}
		fullKey := fmt.Sprintf("spark.%s", key)
		if key == "catalog" {
			fullKey = adbc.OptionKeyCurrentCatalog
		}
		if len(values) != 1 {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[spark] Key '%s' needs to have exactly one value", key),
			}
		}
		options[fullKey] = values[0]
	}

	scheme := uri.Scheme
	api := options[sparkutil.OptionApi]
	switch scheme {
	case "sc", sparkutil.OptionValueApiConnect:
		// Spark Connect's native URI scheme is `sc`. Map it onto our internal
		// api-name convention so `sc://host:port` works out of the box.
		if api != "" && api != sparkutil.OptionValueApiConnect {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[spark] URI scheme 'sc' cannot be used with explicit API option %s=%s", sparkutil.OptionApi, api),
			}
		}
		options[sparkutil.OptionApi] = sparkutil.OptionValueApiConnect
	case "spark":
		// This scheme doesn't imply an API; default to thrift+http if
		// it isn't already set

		// example:
		// spark:// => thrift + binary
		// spark://?api=thrift%2Bhttp => thrift + http
		if _, ok := options[sparkutil.OptionApi]; !ok {
			options[sparkutil.OptionApi] = sparkutil.OptionValueApiThriftBinary
		}
	case sparkutil.OptionValueApiThriftBinary, sparkutil.OptionValueApiThriftHttp, sparkutil.OptionValueApiLivy:
		if api != "" && api != scheme {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[spark] URI scheme '%s' cannot be used with explicit API option %s=%s", scheme, sparkutil.OptionApi, api),
			}
		}
		options[sparkutil.OptionApi] = scheme
	}

	if uri.Path != "" && uri.Path != "/" {
		options[sparkutil.OptionLivyBaseURL] = uri.Path
	}

	return nil
}

func parseHostPortFromOptions(options map[string]string) (string, error) {
	host, ok := options[sparkutil.OptionHost]
	if !ok {
		return "", sparkbase.MissingRequiredOptionErr(sparkutil.OptionHost)
	}
	delete(options, sparkutil.OptionHost)

	if port, hasPort := options[sparkutil.OptionPort]; hasPort {
		delete(options, sparkutil.OptionPort)
		host = fmt.Sprintf("%s:%s", host, port)
	}
	return host, nil
}

func parseIntegerOption(key string, options map[string]string, defaultValue uint16) (uint16, error) {
	opt, ok := options[key]
	if !ok {
		return defaultValue, nil
	}
	delete(options, key)

	intOpt, err := strconv.ParseUint(opt, 10, 16)
	if err != nil {
		return 0, sparkbase.InvalidOptionErr(key, opt)
	}

	return uint16(intOpt), nil
}

func parseBoolOption(key string, options map[string]string, defaultValue bool) (bool, error) {
	opt, ok := options[key]
	if !ok {
		return defaultValue, nil
	}
	delete(options, key)
	opt = strings.ToLower(opt)
	switch opt {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return false, sparkbase.InvalidOptionErr(key, opt)
	}
}

// initializeAws sets up AWS configuration for SigV4 authentication
func awsConfigFromOptions(ctx context.Context, options map[string]string) (aws.Config, error) {
	// Check if explicit credentials are provided
	accessKey := options[sparkutil.OptionLivyAWSAccessKeyID]
	secretKey := options[sparkutil.OptionLivyAWSSecretAccessKey]
	sessionToken := options[sparkutil.OptionLivyAWSSessionToken]
	delete(options, sparkutil.OptionLivyAWSAccessKeyID)
	delete(options, sparkutil.OptionLivyAWSSecretAccessKey)
	delete(options, sparkutil.OptionLivyAWSSessionToken)

	var loadOpts []func(*awsconfig.LoadOptions) error

	// Set region if provided
	if region, ok := options[sparkutil.OptionLivyAWSRegion]; ok {
		loadOpts = append(loadOpts, awsconfig.WithRegion(region))
		delete(options, sparkutil.OptionLivyAWSRegion)
	}

	// Set profile if specified
	if profile := options[sparkutil.OptionLivyAWSProfile]; profile != "" {
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(profile))
	}

	// Use explicit credentials if provided
	if accessKey != "" && secretKey != "" {
		credProvider := awscredentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(credProvider))
	}

	// Load AWS config
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return cfg, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  fmt.Sprintf("failed to load AWS config: %v", err),
		}
	}

	return cfg, nil
}

func livyOptsFromOptions(ctx context.Context, options map[string]string) (livyimpl.ConnectionOpts, error) {
	livyOpts := livyimpl.ConnectionOpts{}

	host, err := parseHostPortFromOptions(options)
	if err != nil {
		return livyOpts, err
	}

	tls, err := parseBoolOption(sparkutil.OptionUseTls, options, false)
	if err != nil {
		return livyOpts, err
	}

	validateServerCertificate, err := parseBoolOption(sparkutil.OptionValidateServerCertificate, options, true)
	if err != nil {
		return livyOpts, err
	}
	livyOpts.ValidateServerCertificate = validateServerCertificate

	if !strings.Contains(host, "://") {
		if tls {
			host = fmt.Sprintf("https://%s", host)
		} else {
			host = fmt.Sprintf("http://%s", host)
		}
	}
	baseURL := options[sparkutil.OptionLivyBaseURL]
	delete(options, sparkutil.OptionLivyBaseURL)
	livyOpts.BaseURL = fmt.Sprintf("%s%s", host, baseURL)

	timeout, err := parseIntegerOption(sparkutil.OptionLivyTimeout, options, 0)
	if err != nil {
		return livyOpts, err
	}
	livyOpts.HttpTimeoutSeconds = uint(timeout)
	// TODO(serramatutu): query timeout
	livyOpts.QueryTimeoutSeconds = 0

	if sessionTtl, ok := options[sparkutil.OptionLivySessionTTL]; ok {
		delete(options, sparkutil.OptionLivySessionTTL)
		livyOpts.SessionTtl = sessionTtl
	}

	if sessionId, ok := options[sparkutil.OptionLivySessionId]; ok {
		delete(options, sparkutil.OptionLivySessionId)
		// Session ids are opaque strings (Microsoft Fabric uses GUIDs, Apache
		// Livy uses integers) so pass them through without parsing.
		livyOpts.ExistingSessionId = &sessionId
	}

	if deleteSession, ok := options[sparkutil.OptionLivyReleaseSession]; ok {
		delete(options, sparkutil.OptionLivyReleaseSession)
		deleteSessionBool, err := strconv.ParseBool(deleteSession)
		if err != nil {
			return livyOpts, sparkbase.InvalidOptionErr(sparkutil.OptionLivyReleaseSession, deleteSession)
		}
		livyOpts.DeleteSessionOnClose = deleteSessionBool
	} else {
		livyOpts.DeleteSessionOnClose = true
	}

	sessionKind, ok := options[sparkutil.OptionLivySessionKind]
	if !ok {
		return livyOpts, sparkbase.MissingRequiredOptionErr(sparkutil.OptionLivySessionKind)
	}
	delete(options, sparkutil.OptionLivySessionKind)
	switch sessionKind {
	case sparkutil.OptionValueSessionKindSql, sparkutil.OptionValueSessionKindSpark, sparkutil.OptionValueSessionKindPySpark:
		livyOpts.SessionKind = livyimpl.SessionKind(sessionKind)
	default:
		return livyOpts, sparkbase.InvalidOptionErr(sparkutil.OptionLivySessionKind, sessionKind)
	}

	authType, ok := options[sparkutil.OptionAuthType]
	if !ok {
		livyOpts.AuthType = livyimpl.AuthTypeNone
	} else {
		delete(options, sparkutil.OptionAuthType)
		switch authType {
		case sparkutil.OptionValueAuthTypeNone:
			livyOpts.AuthType = livyimpl.AuthTypeNone
		case sparkutil.OptionValueAuthTypeBasic:
			livyOpts.AuthType = livyimpl.AuthTypeBasic
		case sparkutil.OptionValueAuthTypeAwsSigv4:
			livyOpts.AuthType = livyimpl.AuthTypeAwsSigV4
			cfg, err := awsConfigFromOptions(ctx, options)
			if err != nil {
				return livyOpts, err
			}
			livyOpts.AwsConfig = cfg
		case sparkutil.OptionValueAuthTypeAzureToken:
			livyOpts.AuthType = livyimpl.AuthTypeAzureToken
			for opt, dst := range map[string]*string{
				sparkutil.OptionLivyAzureCredential: &livyOpts.AzureCredential,
				sparkutil.OptionLivyAzureTokenScope: &livyOpts.AzureTokenScope,
			} {
				if v, ok := options[opt]; ok {
					*dst = v
					delete(options, opt)
				}
			}
		default:
			return livyOpts, sparkbase.InvalidOptionErr(sparkutil.OptionAuthType, authType)
		}
	}

	username := options[adbc.OptionKeyUsername]
	livyOpts.Username = username
	delete(options, adbc.OptionKeyUsername)

	password := options[adbc.OptionKeyPassword]
	livyOpts.Password = password
	delete(options, adbc.OptionKeyPassword)

	// AWS-specific options
	if executionRole, ok := options[sparkutil.OptionLivyAWSExecutionRoleArn]; ok {
		delete(options, sparkutil.OptionLivyAWSExecutionRoleArn)
		options[sparkutil.OptionSparkConfigPrefix+"emr-serverless.session.executionRoleArn"] = executionRole
	}

	return livyOpts, nil
}

func connectOptsFromOptions(options map[string]string) (connectimpl.ConnectionOpts, error) {
	connectOpts := connectimpl.ConnectionOpts{}

	host, err := parseHostPortFromOptions(options)
	if err != nil {
		return connectOpts, err
	}
	connectOpts.Host = host

	username, hasUsername := options[adbc.OptionKeyUsername]
	if hasUsername {
		connectOpts.Username = username
		delete(options, adbc.OptionKeyUsername)
	}
	password, hasPassword := options[adbc.OptionKeyPassword]
	if hasPassword {
		delete(options, adbc.OptionKeyPassword)
	}

	tls, err := parseBoolOption(sparkutil.OptionUseTls, options, false)
	if err != nil {
		return connectOpts, err
	}
	connectOpts.Tls = tls

	validateServerCertificate, err := parseBoolOption(sparkutil.OptionValidateServerCertificate, options, true)
	if err != nil {
		return connectOpts, err
	}
	connectOpts.ValidateServerCertificate = validateServerCertificate

	if sessionID, ok := options[sparkutil.OptionConnectSessionId]; ok {
		connectOpts.SessionID = sessionID
		delete(options, sparkutil.OptionConnectSessionId)
	}

	releaseSession, err := parseBoolOption(sparkutil.OptionConnectReleaseSession, options, true)
	if err != nil {
		return connectOpts, err
	}
	connectOpts.ReleaseSession = releaseSession

	authType, ok := options[sparkutil.OptionAuthType]
	if !ok {
		return connectOpts, sparkbase.MissingRequiredOptionErr(sparkutil.OptionAuthType)
	}
	delete(options, sparkutil.OptionAuthType)
	switch authType {
	case sparkutil.OptionValueAuthTypeNone:
		connectOpts.AuthType = connectimpl.AuthTypeNone
		if password != "" {
			return connectOpts, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[spark] password provided but auth type is '%s'", authType),
			}
		}
	case sparkutil.OptionValueAuthTypeToken:
		connectOpts.AuthType = connectimpl.AuthTypeToken
		if !hasPassword || password == "" {
			return connectOpts, sparkbase.MissingRequiredOptionErr(adbc.OptionKeyPassword)
		}
		if isAwsEmrServerlessHost(connectOpts.Host) {
			connectOpts.Username = ""
			connectOpts.AwsProxyAuth = password
		} else {
			connectOpts.Token = password
		}
	default:
		return connectOpts, sparkbase.InvalidOptionErr(sparkutil.OptionAuthType, authType)
	}

	return connectOpts, nil
}

func isAwsEmrServerlessHost(host string) bool {
	host = strings.ToLower(host)
	return strings.Contains(host, "amazonaws.com") && strings.Contains(host, "emr-serverless-services")
}

func thriftOptsFromOptions(api string, options map[string]string) (thriftimpl.ConnectionOpts, error) {
	thriftOpts := thriftimpl.ConnectionOpts{}
	switch api {
	case sparkutil.OptionValueApiThriftBinary:
		thriftOpts.Transport = thriftimpl.Binary
	case sparkutil.OptionValueApiThriftHttp:
		thriftOpts.Transport = thriftimpl.Http
	default:
		return thriftOpts, sparkbase.InvalidOptionErr(sparkutil.OptionApi, api)
	}

	authType, ok := options[sparkutil.OptionAuthType]
	if !ok {
		return thriftOpts, sparkbase.MissingRequiredOptionErr(sparkutil.OptionAuthType)
	}
	delete(options, sparkutil.OptionAuthType)

	host, err := parseHostPortFromOptions(options)
	if err != nil {
		return thriftOpts, err
	}
	thriftOpts.Host = host

	tls, err := parseBoolOption(sparkutil.OptionUseTls, options, false)
	if err != nil {
		return thriftOpts, err
	}
	thriftOpts.Tls = tls

	validateServerCertificate, err := parseBoolOption(sparkutil.OptionValidateServerCertificate, options, true)
	if err != nil {
		return thriftOpts, err
	}
	thriftOpts.ValidateServerCertificate = validateServerCertificate

	switch authType {
	case sparkutil.OptionValueAuthTypeNoSasl:
		thriftOpts.Auth = thriftimpl.NoSasl
	case sparkutil.OptionValueAuthTypePlain:
		thriftOpts.Auth = thriftimpl.Plain

		username := options[adbc.OptionKeyUsername]
		thriftOpts.Username = username
		delete(options, adbc.OptionKeyUsername)

		password := options[adbc.OptionKeyPassword]
		thriftOpts.Password = password
		delete(options, adbc.OptionKeyPassword)

	case sparkutil.OptionValueAuthTypeLdap, sparkutil.OptionValueAuthTypeKerberos:
		return thriftOpts, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[spark] auth type '%s' has not been implemented yet", authType),
		}
	default:
		return thriftOpts, sparkbase.InvalidOptionErr(sparkutil.OptionAuthType, authType)
	}

	return thriftOpts, nil
}

func sessionOptionsFromOptions(options map[string]string, catalog string) map[string]string {
	sessionOptions := make(map[string]string)
	for k, v := range options {
		if after, ok := strings.CutPrefix(k, sparkutil.OptionSparkConfigPrefix); ok {
			trimmedKey := after
			sessionOptions[trimmedKey] = v
			delete(options, k)
		}
	}
	if catalog != "" {
		sessionOptions["spark.sql.defaultCatalog"] = catalog
	}
	return sessionOptions
}

func validateStartupCatalog(factory sparkbase.SparkClientFactory, catalog string) sparkbase.SparkClientFactory {
	if catalog == "" {
		return factory
	}

	return func(ctx context.Context) (sparkbase.SparkClient, error) {
		client, err := factory(ctx)
		if err != nil {
			return nil, err
		}

		currentCatalog, err := client.CurrentCatalog(ctx, memory.DefaultAllocator)
		if err != nil {
			_ = client.Close(ctx)
			return nil, adbc.Error{
				Code: adbc.StatusNotFound,
				Msg:  fmt.Sprintf("[spark] catalog not found: %s: %s", catalog, err),
			}
		}
		if currentCatalog == catalog {
			return client, nil
		}

		_ = client.Close(ctx)
		return nil, adbc.Error{
			Code: adbc.StatusNotFound,
			Msg:  fmt.Sprintf("[spark] catalog not found: %s", catalog),
		}
	}
}

func newSparkClientFactory(ctx context.Context, options map[string]string) (func(context.Context) (sparkbase.SparkClient, error), error) {
	uri, ok := options[adbc.OptionKeyURI]
	if ok {
		parsed, err := url.Parse(uri)
		if err != nil {
			return nil, sparkbase.ErrToAdbcErr(adbc.StatusInvalidArgument, err, "parse URI")
		}

		err = parseOptionsFromUri(parsed, options)
		if err != nil {
			return nil, sparkbase.ErrToAdbcErr(adbc.StatusInvalidArgument, err, "parse URI")
		}

		delete(options, adbc.OptionKeyURI)
	}

	catalog := options[adbc.OptionKeyCurrentCatalog]
	delete(options, adbc.OptionKeyCurrentCatalog)

	api, ok := options[sparkutil.OptionApi]
	if !ok {
		return nil, sparkbase.MissingRequiredOptionErr(sparkutil.OptionApi)
	}
	delete(options, sparkutil.OptionApi)

	switch api {
	case sparkutil.OptionValueApiThriftBinary, sparkutil.OptionValueApiThriftHttp:
		thriftOpts, err := thriftOptsFromOptions(api, options)
		if err != nil {
			return nil, err
		}

		sessionOptions := sessionOptionsFromOptions(options, catalog)
		factory := func(ctx context.Context) (sparkbase.SparkClient, error) {
			return thriftimpl.NewClient(ctx, thriftOpts, sessionOptions)
		}
		return validateStartupCatalog(factory, catalog), nil

	case sparkutil.OptionValueApiLivy:
		livyOpts, err := livyOptsFromOptions(ctx, options)
		if err != nil {
			return nil, err
		}

		sessionOptions := sessionOptionsFromOptions(options, catalog)
		factory := func(ctx context.Context) (sparkbase.SparkClient, error) {
			return livyimpl.NewClient(ctx, livyOpts, sessionOptions)
		}
		return validateStartupCatalog(factory, catalog), nil

	case sparkutil.OptionValueApiConnect:
		connectOpts, err := connectOptsFromOptions(options)
		if err != nil {
			return nil, err
		}

		sessionOptions := sessionOptionsFromOptions(options, catalog)
		factory := func(ctx context.Context) (sparkbase.SparkClient, error) {
			return connectimpl.NewClient(ctx, connectOpts, sessionOptions)
		}
		return validateStartupCatalog(factory, catalog), nil

	default:
		return nil, sparkbase.InvalidOptionErr(sparkutil.OptionApi, api)
	}
}
