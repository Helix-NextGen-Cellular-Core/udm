// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package consumer

import (
	"context"
	"fmt"
	"hash/fnv"
	"net/http"
	"sort"

	"github.com/antihax/optional"
	nrf_cache "github.com/omec-project/nrf/nrfcache"
	"github.com/omec-project/openapi/Nnrf_NFDiscovery"
	"github.com/omec-project/openapi/models"
	udm_context "github.com/omec-project/udm/context"
	"github.com/omec-project/udm/logger"
	"github.com/omec-project/udm/util"
)

const (
	NFDiscoveryToUDRParamNone int = iota
	NFDiscoveryToUDRParamSupi
	NFDiscoveryToUDRParamExtGroupId
	NFDiscoveryToUDRParamGpsi
)

// func SendNFIntances(nrfUri string, targetNfType, requestNfType models.NfType,
// 	param Nnrf_NFDiscovery.SearchNFInstancesParamOpts) (result models.SearchResult, err error) {
// 	configuration := Nnrf_NFDiscovery.NewConfiguration()
// 	configuration.SetBasePath(nrfUri) // addr
// 	clientNRF := Nnrf_NFDiscovery.NewAPIClient(configuration)

// 	result, res, err1 := clientNRF.NFInstancesStoreApi.SearchNFInstances(context.TODO(), targetNfType,
// 		requestNfType, &param)
// 	if err1 != nil {
// 		err = err1
// 		return
// 	}
// 	defer func() {
// 		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
// 			logger.Handlelog.Errorf("SearchNFInstances response body cannot close: %+v", rspCloseErr)
// 		}
// 	}()

// 	if res != nil && res.StatusCode == http.StatusTemporaryRedirect {
// 		err = fmt.Errorf("Temporary Redirect For Non NRF Consumer")
// 	}
// 	return
// }

func SendNFInstances(nrfUri string, targetNfType, requestNfType models.NfType,
	param *Nnrf_NFDiscovery.SearchNFInstancesParamOpts) (models.SearchResult, error) {
	if udm_context.UDM_Self().EnableNrfCaching {
		logger.ConsumerLog.Errorf("Retrieving from NRF cache")
		return nrf_cache.SearchNFInstances(nrfUri, targetNfType, requestNfType, param)
	} else {
		return SendNfDiscoveryToNrf(nrfUri, targetNfType, requestNfType, param)
	}
}

func SendNfDiscoveryToNrf(nrfUri string, targetNfType, requestNfType models.NfType,
	param *Nnrf_NFDiscovery.SearchNFInstancesParamOpts) (models.SearchResult, error) {
	// Set client and set url
	configuration := Nnrf_NFDiscovery.NewConfiguration()
	configuration.SetBasePath(nrfUri)
	client := Nnrf_NFDiscovery.NewAPIClient(configuration)

	logger.ConsumerLog.Infof("Inside SendNFDiscoveryToNrf")
	result, res, err := client.NFInstancesStoreApi.SearchNFInstances(context.TODO(), targetNfType, requestNfType, param)
	if res != nil && res.StatusCode == http.StatusTemporaryRedirect {
		err = fmt.Errorf("Temporary Redirect For Non NRF Consumer")
	}
	defer func() {
		if bodyCloseErr := res.Body.Close(); bodyCloseErr != nil {
			err = fmt.Errorf("SearchNFInstances' response body cannot close: %+w", bodyCloseErr)
		}
	}()

	udmSelf := udm_context.UDM_Self()

	for _, nfProfile := range result.NfInstances {
		// checking whether the UDM subscribed to this target nfinstanceid or not
		logger.ConsumerLog.Infof("Rcvd data from NRF %s %s", nfProfile.NfInstanceId, nfProfile.NfType)
		if _, ok := udmSelf.NfStatusSubscriptions.Load(nfProfile.NfInstanceId); !ok {
			nrfSubscriptionData := models.NrfSubscriptionData{
				NfStatusNotificationUri: fmt.Sprintf("%s/nf-status-notify", udmSelf.GetIPv4Uri()),
				SubscrCond:              &models.NfInstanceIdCond{NfInstanceId: nfProfile.NfInstanceId},
				ReqNfType:               requestNfType,
			}
			nrfSubData, problemDetails, err := SendCreateSubscription(nrfUri, nrfSubscriptionData)
			if problemDetails != nil {
				logger.ConsumerLog.Errorf("SendCreateSubscription to NRF, Problem[%+v]", problemDetails)
			} else if err != nil {
				logger.ConsumerLog.Errorf("SendCreateSubscription Error[%+v]", err)
			}
			logger.ConsumerLog.Infof("Storing subscription %s %s %s", udmSelf.GetIPv4Uri(), nfProfile.NfInstanceId, nrfSubData.SubscriptionId)
			udmSelf.NfStatusSubscriptions.Store(nfProfile.NfInstanceId, nrfSubData.SubscriptionId)
		}
	}

	return result, err
}
func SendNFIntancesUDR(id string, types int) string {
	self := udm_context.UDM_Self()
	Uenf, ok := self.UeNfProfile.Load(id)
	if ok {
		nf1 := Uenf.(*models.NfProfile)
		logger.ConsumerLog.Warnln("for Ue: ", id, " found targetNfType ", string(models.NfType_UDR), " NF is: ", *nf1)
		return util.SearchNFServiceUri(*nf1, models.ServiceName_NUDR_DR, models.NfServiceStatus_REGISTERED)
	}
	targetNfType := models.NfType_UDR
	requestNfType := models.NfType_UDM
	localVarOptionals := Nnrf_NFDiscovery.SearchNFInstancesParamOpts{
		ServiceNames: optional.NewInterface([]models.ServiceName{models.ServiceName_NUDR_DR}),
		// 	DataSet: optional.NewInterface(models.DataSetId_SUBSCRIPTION),
	}
	// switch types {
	// case NFDiscoveryToUDRParamSupi:
	// 	localVarOptionals.Supi = optional.NewString(id)
	// case NFDiscoveryToUDRParamExtGroupId:
	// 	localVarOptionals.ExternalGroupIdentity = optional.NewString(id)
	// case NFDiscoveryToUDRParamGpsi:
	// 	localVarOptionals.Gpsi = optional.NewString(id)
	// }
	// fmt.Println(self.NrfUri)
	result, err := SendNFInstances(self.NrfUri, targetNfType, requestNfType, &localVarOptionals)
	if err != nil {
		logger.Handlelog.Error(err.Error())
		return ""
	}
	nfInstanceIds := make([]string, 0, len(result.NfInstances))
	for _, profile := range result.NfInstances {
		nfInstanceIds = append(nfInstanceIds, profile.NfInstanceId)
	}
	sort.Strings(nfInstanceIds)

	nfInstanceIdIndexMap := make(map[string]int)
	for index, value := range nfInstanceIds {
		nfInstanceIdIndexMap[value] = index
	}
	nfInstanceIndex := 0
	if self.EnableScaling {
		h := fnv.New32a()
		h.Write([]byte(id))
		key := int(h.Sum32())
		nfInstanceIndex = key % len(result.NfInstances)
	}
	for _, profile := range result.NfInstances {
		if nfInstanceIndex != nfInstanceIdIndexMap[profile.NfInstanceId] {
			continue
		}
		self.UeNfProfile.Store(id, &profile)
		logger.ConsumerLog.Warnln("for Ue: ", id, " for targetNfType ", string(models.NfType_UDR), " NF is: ", profile)

		return util.SearchNFServiceUri(profile, models.ServiceName_NUDR_DR, models.NfServiceStatus_REGISTERED)
	}

	// min := int(math.MaxInt64)
	// nf := models.NfProfile{}
	// self.NfProfileListMutex.Lock()
	// if self.NfProfileList[models.NfType_UDR] == nil {
	// 	self.NfProfileList[models.NfType_UDR] = make(map[string]int)
	// }
	// for _, nfProfile := range result.NfInstances {
	// 	if self.NfProfileList[models.NfType_UDR][nfProfile.NfInstanceId] < min {
	// 		nf = nfProfile
	// 		min = self.NfProfileList[models.NfType_UDR][nfProfile.NfInstanceId]
	// 	}
	// }
	// self.NfProfileList[models.NfType_UDR][nf.NfInstanceId]++
	// count := self.NfProfileList[models.NfType_UDR][nf.NfInstanceId]
	// self.UeNfProfile.Store(id, &nf)
	// self.NfProfileListMutex.Unlock()
	// logger.ConsumerLog.Warnln("for Ue: ", id, " for targetNfType ", string(models.NfType_UDR), " NF is: ", nf, "nf Count:", count)

	// return util.SearchNFServiceUri(profile, models.ServiceName_NUDR_DR, models.NfServiceStatus_REGISTERED)
	return ""
}
