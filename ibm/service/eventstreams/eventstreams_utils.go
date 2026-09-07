// Copyright IBM Corp. 2026 All Rights Reserved.
// Licensed under the Mozilla Public License v2.0

package eventstreams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/IBM-Cloud/bluemix-go/session"
	"github.com/IBM-Cloud/terraform-provider-ibm/ibm/conns"
	"github.com/IBM-Cloud/terraform-provider-ibm/ibm/flex"
	"github.com/IBM-Cloud/terraform-provider-ibm/version"
	"github.com/IBM/eventstreams-go-sdk/pkg/adminrestv1"
	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/platform-services-go-sdk/resourcecontrollerv2"
	"github.com/IBM/sarama"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// clientPool maintains Kafka admin client for each instance.
// key is instance's CRN
var clientPool = map[string]sarama.ClusterAdmin{}

type extensions struct {
	platformGeneration int
	adminURL           string
	bootstrapServers   []string
}

// formatObject flattens the map representation of a JSON object into a string.
// Property names are preserved, but values are replaced by their Go type.
// Intent is to provide debug information for service extensions without logging
// values.
func formatObject(m map[string]interface{}) string {
	sb := &strings.Builder{}
	for idx, propertyName := range slices.Sorted(maps.Keys(m)) {
		if idx != 0 {
			sb.WriteString(",")
		}
		value := m[propertyName]
		if mapValue, ok := value.(map[string]interface{}); ok {
			sb.WriteString(fmt.Sprintf("%q:{%s}", propertyName, formatObject(mapValue)))
		} else {
			sb.WriteString(fmt.Sprintf("%q:%T", propertyName, value))
		}
	}
	return sb.String()
}

func parseInstanceExtensions(instance *resourcecontrollerv2.ResourceInstance, meta interface{}) (*extensions, error) {
	if kafkaHTTP, ok := instance.Extensions["kafka_http_url"].(string); ok {
		if brokersSASL, ok := instance.Extensions["kafka_brokers_sasl"].([]interface{}); ok {
			bootstrapServers := flex.ExpandStringList(brokersSASL)
			slices.Sort(bootstrapServers)
			return &extensions{
				adminURL:           kafkaHTTP,
				bootstrapServers:   bootstrapServers,
				platformGeneration: 1,
			}, nil
		}
	}
	if dataservices, ok := instance.Extensions["dataservices"].(map[string]any); ok {
		if connection, ok := dataservices["connection"].(map[string]any); ok {
			if bootstrapServers, ok := connection["bootstrap_servers"].(string); ok {
				if restURL, ok := connection["rest_url"].(string); ok {
					return &extensions{
						adminURL:           restURL,
						bootstrapServers:   strings.Split(bootstrapServers, ","),
						platformGeneration: 2,
					}, nil
				}
			}
		}
	}
	log.Printf("[DEBUG] unexpected format of instance extensions: %s", formatObject(instance.Extensions))
	return nil, errors.New("unexpected format of instance extensions")
}

func createSaramaAdminClient(d *schema.ResourceData, meta interface{}) (sarama.ClusterAdmin, *extensions, string, error) {
	instanceCRN := d.Get("resource_instance_id").(string)
	if len(instanceCRN) == 0 {
		id := d.Id()
		if len(id) == 0 || !strings.Contains(id, ":") {
			log.Printf("[DEBUG] createSaramaAdminClient resource_instance_id is missing")
			return nil, nil, "", fmt.Errorf("resource_instance_id is required")
		}
		instanceCRN = getInstanceCRN(id)
	}
	instance, err := getInstanceDetails(instanceCRN, meta)
	if err != nil {
		log.Printf("[DEBUG] createSaramaAdminClient err %s", err)
		return nil, nil, "", err
	}
	ext, err := parseInstanceExtensions(instance, meta)
	if err != nil {
		return nil, nil, "", err
	}
	adminClient, err := newSaramaAdminClient(instanceCRN, ext, meta)
	if err != nil {
		return nil, nil, "", err
	}
	return adminClient, ext, instanceCRN, nil
}

func newSaramaAdminClient(instanceCRN string, ext *extensions, meta interface{}) (sarama.ClusterAdmin, error) {
	if adminClient, ok := clientPool[instanceCRN]; ok {
		log.Printf("[DEBUG] newSaramaAdminClient got client from pool for instance %s", instanceCRN)
		return adminClient, nil
	}
	bxSession, err := meta.(conns.ClientSession).BluemixSession()
	if err != nil {
		log.Printf("[DEBUG] newSaramaAdminClient BluemixSession err %s", err)
		return nil, err
	}
	log.Printf("[INFO] newSaramaAdminClient kafka_http_url is set to %s", ext.adminURL)
	log.Printf("[INFO] newSaramaAdminClient kafka_brokers_sasl is set to %s", strings.Join(ext.bootstrapServers, ","))
	config := sarama.NewConfig()
	config.ClientID = fmt.Sprintf("terraform-provider-ibm/%s", version.Version)
	config.Net.SASL.Enable = true
	config.Net.TLS.Enable = true
	config.Version = sarama.V3_8_1_0
	if ext.platformGeneration >= 2 {
		config.Version = sarama.V4_1_0_0
	}
	tenantID := strings.TrimPrefix(strings.Split(ext.adminURL, ".")[0], "https://")
	if ext.platformGeneration == 1 && tenantID != "" && tenantID != "admin" {
		config.Net.SASL.AuthIdentity = tenantID
	} else {
		config.Net.SASL.AuthIdentity = instanceCRN
	}
	config.Admin.Timeout = adminClientTimeout
	config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	config.Net.SASL.TokenProvider, err = newAccessTokenProvider(bxSession)
	if err != nil {
		return nil, err
	}
	adminClient, err := sarama.NewClusterAdmin(ext.bootstrapServers, config)
	if err != nil {
		log.Printf("[DEBUG] newSaramaAdminClient NewClusterAdmin err %s", err)
		return nil, err
	}
	clientPool[instanceCRN] = adminClient
	log.Printf("[INFO] newSaramaAdminClient instance %s's client is initialized", instanceCRN)
	return adminClient, nil
}

func topicDetail2Config(topicConfigEntries map[string]*string) map[string]*string {
	configs := map[string]*string{}
	for key, value := range topicConfigEntries {
		if flex.IndexOf(key, allowedTopicConfigs) != -1 {
			configs[key] = value
		}
	}
	return configs
}

func config2TopicDetail(config map[string]interface{}) map[string]*string {
	configEntries := make(map[string]*string)
	for key, value := range config {
		switch value := value.(type) {
		case string:
			configEntries[key] = &value
		}
	}
	return configEntries
}

func getTopicID(instanceCRN string, topicName string) string {
	crnSegments := strings.Split(instanceCRN, ":")
	crnSegments[8] = "topic"
	crnSegments[9] = topicName
	return strings.Join(crnSegments, ":")
}

func getTopicName(topicID string) string {
	return strings.Split(topicID, ":")[9]
}

func getInstanceCRN(topicID string) string {
	crnSegments := strings.Split(topicID, ":")
	crnSegments[8] = ""
	crnSegments[9] = ""
	return strings.Join(crnSegments, ":")
}

type accessTokenProvider struct {
	authenticator *core.IamAuthenticator
}

func newAccessTokenProvider(sess *session.Session) (*accessTokenProvider, error) {
	iamEndpoint, err := sess.Config.EndpointLocator.IAMEndpoint()
	if err != nil {
		log.Printf("[DEBUG] newAccessTokenProvider.IAMEndpoint() error:%s", err)
		return nil, err
	}
	authenticator, err := core.NewIamAuthenticatorBuilder().
		SetURL(iamEndpoint).
		SetApiKey(sess.Config.BluemixAPIKey).
		SetRefreshToken(sess.Config.IAMRefreshToken).
		SetClientIDSecret("bx", "bx").
		Build()
	if err != nil {
		log.Printf("[DEBUG] newAccessTokenProvider.NewIamAuthenticatorBuilder() error:%s", err)
		return nil, err
	}
	return &accessTokenProvider{authenticator}, nil
}

// Token() implements sarama.AccessTokenProvider interface for sasl.mechanism=OAUTHBEARER
func (tp *accessTokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := tp.authenticator.GetToken()
	if err != nil {
		log.Printf("[DEBUG] accessTokenProvider.GetToken() error:%s", err)
		return nil, err
	}
	return &sarama.AccessToken{Token: token}, nil
}

type (
	QuotaClient interface {
		CreateQuota(ctx context.Context, name string, details QuotaDetails) error
		GetQuota(ctx context.Context, name string) (*QuotaDetails, error)
		UpdateQuota(ctx context.Context, name string, newDetails QuotaDetails) error
		DeleteQuota(ctx context.Context, name string) error
	}

	QuotaDetails struct {
		ProducerByteRate *int64
		ConsumerByteRate *int64
	}
)

type quotaClientError struct {
	notFound bool
	response *string
	err      error
}

func (e *quotaClientError) Error() string {
	return e.err.Error()
}

func quotaClientResponse(err error) string {
	if quotaClientErr, ok := errors.AsType[*quotaClientError](err); ok {
		if quotaClientErr.response != nil {
			return *quotaClientErr.response
		}
		// Marshal error message into JSON object
		data, err := json.Marshal(map[string]string{"message": err.Error()})
		if err != nil {
			return "{}"
		}
		return string(data)
	}
	return "{}"
}

func quotaClientIsNotFound(err error) bool {
	if quotaClientErr, ok := errors.AsType[*quotaClientError](err); ok {
		return quotaClientErr.notFound
	}
	return false
}

type quotaClientAdminRESTWrapper struct {
	adminRESTClient *adminrestv1.AdminrestV1
}

func (c *quotaClientAdminRESTWrapper) newQuotaClientError(response *core.DetailedResponse, err error) error {
	result := &quotaClientError{
		err: err,
	}
	if response != nil {
		result.notFound = response.StatusCode == http.StatusNotFound
		result.response = new(response.String())
	} else if err != nil {
		result.response = new(err.Error())
	}
	return result
}

func (c *quotaClientAdminRESTWrapper) CreateQuota(ctx context.Context, name string, details QuotaDetails) error {
	response, err := c.adminRESTClient.CreateQuotaWithContext(ctx, &adminrestv1.CreateQuotaOptions{
		EntityName:       &name,
		ConsumerByteRate: details.ConsumerByteRate,
		ProducerByteRate: details.ProducerByteRate,
	})
	if err != nil {
		return c.newQuotaClientError(response, err)
	}
	return nil
}

func (c *quotaClientAdminRESTWrapper) GetQuota(ctx context.Context, name string) (*QuotaDetails, error) {
	result, response, err := c.adminRESTClient.GetQuotaWithContext(ctx, &adminrestv1.GetQuotaOptions{
		EntityName: &name,
	})
	if err != nil {
		return nil, c.newQuotaClientError(response, err)
	}
	return &QuotaDetails{
		ConsumerByteRate: result.ConsumerByteRate,
		ProducerByteRate: result.ProducerByteRate,
	}, nil
}

func (c *quotaClientAdminRESTWrapper) UpdateQuota(ctx context.Context, name string, newDetails QuotaDetails) error {
	response, err := c.adminRESTClient.UpdateQuotaWithContext(ctx, &adminrestv1.UpdateQuotaOptions{
		EntityName:       &name,
		ConsumerByteRate: newDetails.ConsumerByteRate,
		ProducerByteRate: newDetails.ProducerByteRate,
	})
	if err != nil {
		return c.newQuotaClientError(response, err)
	}
	return nil
}

func (c *quotaClientAdminRESTWrapper) DeleteQuota(ctx context.Context, name string) error {
	response, err := c.adminRESTClient.DeleteQuotaWithContext(ctx, &adminrestv1.DeleteQuotaOptions{
		EntityName: &name,
	})
	if err != nil {
		return c.newQuotaClientError(response, err)
	}
	return nil
}

type quotaClientSaramaWrapper struct {
	admin sarama.ClusterAdmin
}

func (c *quotaClientSaramaWrapper) convertNameToMatch(name string) (match string, matchType sarama.QuotaMatchType) {
	if name == "default" {
		return "", sarama.QuotaMatchDefault
	}
	return name, sarama.QuotaMatchExact
}

func (c *quotaClientSaramaWrapper) getController() (*sarama.Broker, error) {
	controller, err := c.admin.Controller()
	if err != nil {
		return nil, &quotaClientError{
			response: new(fmt.Sprintf("failed to get Kafka controller: %s", err.Error())),
			err:      err,
		}
	}
	return controller, nil
}

func (c *quotaClientSaramaWrapper) getQuotaEntries(name string) ([]sarama.DescribeClientQuotasEntry, error) {
	match, matchType := c.convertNameToMatch(name)
	quotaFilterComponent := sarama.QuotaFilterComponent{
		EntityType: sarama.QuotaEntityUser,
		Match:      match,
		MatchType:  matchType,
	}
	entries, err := c.admin.DescribeClientQuotas([]sarama.QuotaFilterComponent{
		quotaFilterComponent,
	}, false)
	if err != nil {
		return nil, &quotaClientError{
			response: new(fmt.Sprintf("failed to get details of quota: %q", name)),
			err:      err,
		}
	}
	return entries, err
}

func (c *quotaClientSaramaWrapper) alterQuota(name string, ops []sarama.ClientQuotasOp) error {
	controller, err := c.getController()
	if err != nil {
		return err
	}
	entityName, matchType := c.convertNameToMatch(name)
	response, err := controller.AlterClientQuotas(&sarama.AlterClientQuotasRequest{
		Entries: []sarama.AlterClientQuotasEntry{
			{
				Entity: []sarama.QuotaEntityComponent{{
					EntityType: sarama.QuotaEntityUser,
					MatchType:  matchType,
					Name:       entityName,
				}},
				Ops: ops,
			},
		},
		Version: 1,
	})
	if err != nil {
		return &quotaClientError{
			response: new(fmt.Sprintf("failed to create/update quota: %q", name)),
			err:      err,
		}
	}
	for _, entry := range response.Entries {
		if entry.ErrorMsg != nil && len(*entry.ErrorMsg) > 0 {
			return &quotaClientError{
				response: new(fmt.Sprintf("failed to create/update quota %q: %s", name, *entry.ErrorMsg)),
				err:      errors.New(*entry.ErrorMsg),
			}
		} else if entry.ErrorCode != sarama.ErrNoError {
			return &quotaClientError{
				response: new(fmt.Sprintf("failed to create/update quota %q: error code %d", name, entry.ErrorCode)),
				err:      fmt.Errorf("kafka API error code %d", entry.ErrorCode),
			}
		}
	}
	return nil
}

func (c *quotaClientSaramaWrapper) buildQuotaOptions(details QuotaDetails) []sarama.ClientQuotasOp {
	ops := make([]sarama.ClientQuotasOp, 0)
	if details.ConsumerByteRate != nil {
		if *details.ConsumerByteRate >= 0 {
			ops = append(ops, sarama.ClientQuotasOp{
				Key:   consumerByteRateKey,
				Value: float64(*details.ConsumerByteRate),
			})
		} else {
			ops = append(ops, sarama.ClientQuotasOp{
				Key:    consumerByteRateKey,
				Remove: true,
			})
		}
	}
	if details.ProducerByteRate != nil {
		if *details.ProducerByteRate >= 0 {
			ops = append(ops, sarama.ClientQuotasOp{
				Key:   producerByteRateKey,
				Value: float64(*details.ProducerByteRate),
			})
		} else {
			ops = append(ops, sarama.ClientQuotasOp{
				Key:    producerByteRateKey,
				Remove: true,
			})
		}
	}
	return ops
}

func (c *quotaClientSaramaWrapper) quotaNotFoundError(name string) *quotaClientError {
	return &quotaClientError{
		notFound: true,
		response: new(fmt.Sprintf("quota %q not found", name)),
		err:      errors.New("quota not found"),
	}
}

func (c *quotaClientSaramaWrapper) CreateQuota(ctx context.Context, name string, details QuotaDetails) error {
	entries, err := c.getQuotaEntries(name)
	if err != nil {
		return err
	}

	if len(entries) > 0 {
		return &quotaClientError{
			response: new(fmt.Sprintf("quota %q already exists", name)),
			err:      errors.New("quota already exists"),
		}
	}

	ops := c.buildQuotaOptions(details)
	return c.alterQuota(name, ops)
}

func (c *quotaClientSaramaWrapper) GetQuota(ctx context.Context, name string) (*QuotaDetails, error) {
	entries, err := c.getQuotaEntries(name)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, c.quotaNotFoundError(name)
	}

	matchedQuota := entries[0]
	details := QuotaDetails{}
	if val, ok := matchedQuota.Values[consumerByteRateKey]; ok {
		details.ConsumerByteRate = new(int64(val))
	}
	if val, ok := matchedQuota.Values[producerByteRateKey]; ok {
		details.ProducerByteRate = new(int64(val))
	}
	return &details, nil
}

func (c *quotaClientSaramaWrapper) UpdateQuota(ctx context.Context, name string, newDetails QuotaDetails) error {
	entries, err := c.getQuotaEntries(name)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return c.quotaNotFoundError(name)
	}

	ops := c.buildQuotaOptions(newDetails)
	return c.alterQuota(name, ops)
}

func (c *quotaClientSaramaWrapper) DeleteQuota(ctx context.Context, name string) error {
	entries, err := c.getQuotaEntries(name)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return c.quotaNotFoundError(name)
	}

	return c.alterQuota(name, []sarama.ClientQuotasOp{{
		Key:    consumerByteRateKey,
		Remove: true,
	}, {
		Key:    producerByteRateKey,
		Remove: true,
	}})
}
