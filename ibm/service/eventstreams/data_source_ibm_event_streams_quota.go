// Copyright IBM Corp. 2024, 2026 All Rights Reserved.
// Licensed under the Mozilla Public License v2.0

package eventstreams

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/IBM-Cloud/terraform-provider-ibm/ibm/conns"
	"github.com/IBM-Cloud/terraform-provider-ibm/ibm/flex"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

const (
	consumerByteRateKey = "consumer_byte_rate"
	producerByteRateKey = "producer_byte_rate"
)

// A quota in an Event Streams service instance.
// The ID is the CRN with the last two components "quota:entity".
func DataSourceIBMEventStreamsQuota() *schema.Resource {
	return &schema.Resource{
		ReadContext: dataSourceIBMEventStreamsQuotaRead,

		Schema: map[string]*schema.Schema{
			"resource_instance_id": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The ID or CRN of the Event Streams service instance",
			},
			"entity": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The entity for which the quota is set; 'default' or IAM ID",
			},
			"producer_byte_rate": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "The producer quota in bytes per second, -1 means no quota",
			},
			"consumer_byte_rate": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "The consumer quota in bytes per second, -1 means no quota",
			},
		},
	}
}

// read quota properties using the admin-rest API
func dataSourceIBMEventStreamsQuotaRead(context context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	adminrestClient, instanceCRN, entity, err := getQuotaClientInstanceEntity(d, meta)
	if err != nil {
		tfErr := flex.TerraformErrorf(err, "Error getting Event Streams instance", "ibm_event_streams_quota", "read")
		log.Printf("[DEBUG]\n%s", tfErr.GetDebugMessage())
		return tfErr.GetDiag()
	}

	quota, err := adminrestClient.GetQuota(context, entity)
	if err != nil {
		var tfErr *flex.TerraformProblem
		if quotaClientIsNotFound(err) {
			tfErr = flex.TerraformErrorf(err, fmt.Sprintf("Quota for '%s' does not exist", entity), "ibm_event_streams_quota", "read")
		} else {
			tfErr = flex.TerraformErrorf(err, fmt.Sprintf("GetQuota failed with response: %s", quotaClientResponse(err)), "ibm_event_streams_quota", "read")
		}
		log.Printf("[DEBUG]\n%s", tfErr.GetDebugMessage())
		return tfErr.GetDiag()
	}

	d.Set("resource_instance_id", instanceCRN)
	d.Set("entity", entity)
	d.Set("producer_byte_rate", getQuotaValue(quota.ProducerByteRate))
	d.Set("consumer_byte_rate", getQuotaValue(quota.ConsumerByteRate))
	d.SetId(getQuotaID(instanceCRN, entity))

	return nil
}

// Returns
// admin-rest client (set to use the service instance)
// CRN for the service instance
// entity name
// Any error that occurred
func getQuotaClientInstanceEntity(d *schema.ResourceData, meta interface{}) (QuotaClient, string, string, error) {
	instanceCRN := d.Get("resource_instance_id").(string)
	if instanceCRN == "" { // importing
		id := d.Id()
		crnSegments := strings.Split(id, ":")
		if len(crnSegments) != 10 || crnSegments[8] != "quota" || crnSegments[9] == "" {
			return nil, "", "", fmt.Errorf("ID '%s' is not a quota resource", id)
		}
		entity := crnSegments[9]
		crnSegments[8] = ""
		crnSegments[9] = ""
		instanceCRN = strings.Join(crnSegments, ":")
		d.Set("resource_instance_id", instanceCRN)
		d.Set("entity", entity)
	}

	instance, err := getInstanceDetails(instanceCRN, meta)
	if err != nil {
		return nil, "", "", err
	}

	extensions, err := parseInstanceExtensions(instance, meta)
	if err != nil {
		return nil, "", "", err
	}
	if extensions.platformGeneration == 1 {
		adminrestClient, err := meta.(conns.ClientSession).ESadminRestSession()
		if err != nil {
			return nil, "", "", err
		}
		adminrestClient.SetServiceURL(extensions.adminURL)
		wrappedClient := &quotaClientAdminRESTWrapper{
			adminRESTClient: adminrestClient,
		}
		return wrappedClient, instanceCRN, d.Get("entity").(string), nil
	}
	// else, assume gen2 and use Sarama
	client, err := newSaramaAdminClient(instanceCRN, extensions, meta)
	if err != nil {
		return nil, "", "", err
	}
	wrappedClient := &quotaClientSaramaWrapper{
		admin: client,
	}
	return wrappedClient, instanceCRN, d.Get("entity").(string), nil
}

func getQuotaID(instanceCRN string, entity string) string {
	crnSegments := strings.Split(instanceCRN, ":")
	crnSegments[8] = "quota"
	crnSegments[9] = entity
	return strings.Join(crnSegments, ":")
}

// admin-rest API returns nil for undefined rate, convert that to -1
func getQuotaValue(v *int64) int {
	if v == nil {
		return -1
	}
	return int(*v)
}
