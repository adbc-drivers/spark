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
	"github.com/adbc-drivers/apache/go/internal/connectimpl"
	"github.com/adbc-drivers/apache/go/internal/livyimpl"
	"github.com/adbc-drivers/apache/go/internal/sparkbase"
	"github.com/adbc-drivers/apache/go/internal/thriftimpl"
	"github.com/apache/arrow-adbc/go/adbc"
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
		options[OptionHost] = split[0]
	case 2:
		options[OptionHost] = split[0]
		options[OptionPort] = split[1]
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
		if len(values) != 1 {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[spark] Key '%s' needs to have exactly one value", key),
			}
		}
		options[fullKey] = values[0]
	}

	scheme := uri.Scheme
	api := options[OptionApi]
	switch scheme {
	case "sc", OptionValueApiConnect:
		// Spark Connect's native URI scheme is `sc`. Map it onto our internal
		// api-name convention so `sc://host:port` works out of the box.
		if api != "" && api != OptionValueApiConnect {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[spark] URI scheme 'sc' cannot be used with explicit API option %s=%s", OptionApi, api),
			}
		}
		options[OptionApi] = OptionValueApiConnect
	case "spark":
		// This scheme doesn't imply an API; default to thrift+http if
		// it isn't already set

		// example:
		// spark:// => thrift + binary
		// spark://?api=thrift%2Bhttp => thrift + http
		if _, ok := options[OptionApi]; !ok {
			options[OptionApi] = OptionValueApiThriftBinary
		}
	case OptionValueApiThriftBinary, OptionValueApiThriftHttp, OptionValueApiLivy:
		if api != "" && api != scheme {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[spark] URI scheme '%s' cannot be used with explicit API option %s=%s", scheme, OptionApi, api),
			}
		}
		options[OptionApi] = scheme
	}

	return nil
}

func parseHostPortFromOptions(options map[string]string) (string, error) {
	host, ok := options[OptionHost]
	if !ok {
		return "", sparkbase.MissingRequiredOptionErr(OptionHost)
	}
	delete(options, OptionHost)

	if port, hasPort := options[OptionPort]; hasPort {
		delete(options, OptionPort)
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
	accessKey := options[OptionLivyAWSAccessKeyID]
	secretKey := options[OptionLivyAWSSecretAccessKey]
	sessionToken := options[OptionLivyAWSSessionToken]
	delete(options, OptionLivyAWSAccessKeyID)
	delete(options, OptionLivyAWSSecretAccessKey)
	delete(options, OptionLivyAWSSessionToken)

	var loadOpts []func(*awsconfig.LoadOptions) error

	// Set region if provided
	if region, ok := options[OptionLivyAWSRegion]; ok {
		loadOpts = append(loadOpts, awsconfig.WithRegion(region))
		delete(options, OptionLivyAWSRegion)
	}

	// Set profile if specified
	if profile := options[OptionLivyAWSProfile]; profile != "" {
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

	tls, err := parseBoolOption(OptionUseTls, options, false)
	if err != nil {
		return livyOpts, err
	}

	validateServerCertificate, err := parseBoolOption(OptionValidateServerCertificate, options, true)
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
	livyOpts.BaseURL = host

	timeout, err := parseIntegerOption(OptionLivyTimeout, options, 0)
	if err != nil {
		return livyOpts, err
	}
	livyOpts.HttpTimeoutSeconds = uint(timeout)
	// TODO(serramatutu): query timeout
	livyOpts.QueryTimeoutSeconds = 0

	if sessionTtl, ok := options[OptionLivySessionTTL]; ok {
		delete(options, OptionLivySessionTTL)
		livyOpts.SessionTtl = sessionTtl
	}

	sessionKind, ok := options[OptionLivySessionKind]
	if !ok {
		return livyOpts, sparkbase.MissingRequiredOptionErr(OptionLivySessionKind)
	}
	delete(options, OptionLivySessionKind)
	switch sessionKind {
	case OptionValueSessionKindSql, OptionValueSessionKindSpark, OptionValueSessionKindPySpark:
		livyOpts.SessionKind = livyimpl.SessionKind(sessionKind)
	default:
		return livyOpts, sparkbase.InvalidOptionErr(OptionLivySessionKind, sessionKind)
	}

	authType, ok := options[OptionAuthType]
	if !ok {
		return livyOpts, sparkbase.MissingRequiredOptionErr(OptionAuthType)
	}
	delete(options, OptionAuthType)
	switch authType {
	case OptionValueAuthTypeBasic:
		livyOpts.AuthType = livyimpl.AuthTypeBasic
	case OptionValueAuthTypeAwsSigv4:
		livyOpts.AuthType = livyimpl.AuthTypeAwsSigV4
		cfg, err := awsConfigFromOptions(ctx, options)
		if err != nil {
			return livyOpts, err
		}
		livyOpts.AwsConfig = cfg
	default:
		return livyOpts, sparkbase.InvalidOptionErr(OptionAuthType, authType)
	}

	username := options[adbc.OptionKeyUsername]
	livyOpts.Username = username
	delete(options, adbc.OptionKeyUsername)

	password := options[adbc.OptionKeyPassword]
	livyOpts.Password = password
	delete(options, adbc.OptionKeyPassword)

	// AWS-specific options
	if executionRole, ok := options[OptionLivyAWSExecutionRoleArn]; ok {
		delete(options, OptionLivyAWSExecutionRoleArn)
		options[OptionSparkConfigPrefix+"emr-serverless.session.executionRoleArn"] = executionRole
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

	if username, ok := options[adbc.OptionKeyUsername]; ok {
		delete(options, adbc.OptionKeyUsername)
		connectOpts.Username = username
	}

	// XXX: ignored, because spark-connect-go doesn't let you configure this
	_, err = parseBoolOption(OptionUseTls, options, false)
	if err != nil {
		return connectOpts, err
	}
	_, err = parseBoolOption(OptionValidateServerCertificate, options, true)
	if err != nil {
		return connectOpts, err
	}

	authType, ok := options[OptionAuthType]
	if !ok {
		return connectOpts, sparkbase.MissingRequiredOptionErr(OptionAuthType)
	}
	delete(options, OptionAuthType)
	switch authType {
	case OptionValueAuthTypeNone:
		connectOpts.AuthType = connectimpl.AuthTypeNone
	case OptionValueAuthTypeToken:
		connectOpts.AuthType = connectimpl.AuthTypeToken
		password, hasPassword := options[adbc.OptionKeyPassword]
		if !hasPassword || password == "" {
			return connectOpts, sparkbase.MissingRequiredOptionErr(adbc.OptionKeyPassword)
		}
		delete(options, adbc.OptionKeyPassword)
		connectOpts.Token = password
	default:
		return connectOpts, sparkbase.InvalidOptionErr(OptionAuthType, authType)
	}

	return connectOpts, nil
}

func thriftOptsFromOptions(api string, options map[string]string) (thriftimpl.ConnectionOpts, error) {
	thriftOpts := thriftimpl.ConnectionOpts{}
	switch api {
	case OptionValueApiThriftBinary:
		thriftOpts.Transport = thriftimpl.Binary
	case OptionValueApiThriftHttp:
		thriftOpts.Transport = thriftimpl.Http
	default:
		return thriftOpts, sparkbase.InvalidOptionErr(OptionApi, api)
	}

	authType, ok := options[OptionAuthType]
	if !ok {
		return thriftOpts, sparkbase.MissingRequiredOptionErr(OptionAuthType)
	}
	delete(options, OptionAuthType)

	host, err := parseHostPortFromOptions(options)
	if err != nil {
		return thriftOpts, err
	}
	thriftOpts.Host = host

	tls, err := parseBoolOption(OptionUseTls, options, false)
	if err != nil {
		return thriftOpts, err
	}
	thriftOpts.Tls = tls

	validateServerCertificate, err := parseBoolOption(OptionValidateServerCertificate, options, true)
	if err != nil {
		return thriftOpts, err
	}
	thriftOpts.ValidateServerCertificate = validateServerCertificate

	switch authType {
	case OptionValueAuthTypeNoSasl:
		thriftOpts.Auth = thriftimpl.NoSasl
	case OptionValueAuthTypePlain:
		thriftOpts.Auth = thriftimpl.Plain

		username := options[adbc.OptionKeyUsername]
		thriftOpts.Username = username
		delete(options, adbc.OptionKeyUsername)

		password := options[adbc.OptionKeyPassword]
		thriftOpts.Password = password
		delete(options, adbc.OptionKeyPassword)

	case OptionValueAuthTypeLdap, OptionValueAuthTypeKerberos:
		return thriftOpts, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[spark] auth type '%s' has not been implemented yet", authType),
		}
	default:
		return thriftOpts, sparkbase.InvalidOptionErr(OptionAuthType, authType)
	}

	return thriftOpts, nil
}

func sessionOptionsFromOptions(options map[string]string) map[string]string {
	sessionOptions := make(map[string]string)
	for k, v := range options {
		if after, ok := strings.CutPrefix(k, OptionSparkConfigPrefix); ok {
			trimmedKey := after
			sessionOptions[trimmedKey] = v
			delete(options, k)
		}
	}
	return sessionOptions
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

	api, ok := options[OptionApi]
	if !ok {
		return nil, sparkbase.MissingRequiredOptionErr(OptionApi)
	}
	delete(options, OptionApi)

	switch api {
	case OptionValueApiThriftBinary, OptionValueApiThriftHttp:
		thriftOpts, err := thriftOptsFromOptions(api, options)
		if err != nil {
			return nil, err
		}

		sessionOptions := sessionOptionsFromOptions(options)
		return func(ctx context.Context) (sparkbase.SparkClient, error) {
			return thriftimpl.NewClient(ctx, thriftOpts, sessionOptions)
		}, nil

	case OptionValueApiLivy:
		livyOpts, err := livyOptsFromOptions(ctx, options)
		if err != nil {
			return nil, err
		}

		sessionOptions := sessionOptionsFromOptions(options)
		return func(ctx context.Context) (sparkbase.SparkClient, error) {
			return livyimpl.NewClient(ctx, livyOpts, sessionOptions)
		}, nil

	case OptionValueApiConnect:
		connectOpts, err := connectOptsFromOptions(options)
		if err != nil {
			return nil, err
		}

		sessionOptions := sessionOptionsFromOptions(options)
		return func(ctx context.Context) (sparkbase.SparkClient, error) {
			return connectimpl.NewClient(ctx, connectOpts, sessionOptions)
		}, nil

	default:
		return nil, sparkbase.InvalidOptionErr(OptionApi, api)
	}
}
