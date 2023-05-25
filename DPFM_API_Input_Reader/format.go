package dpfm_api_input_reader

import (
	"data-platform-api-local-region-exconf-rmq-kube/DPFM_API_Caller/requests"
)

func (sdc *SDC) ConvertToLocalRegion() *requests.LocalRegion {
	data := sdc.LocalRegion
	return &requests.LocalRegion{
	    LocalRegion:     data.LocalRegion,
		Country:         data.Country,
	}
}
