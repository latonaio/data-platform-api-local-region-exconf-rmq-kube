package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-local-region-exconf-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-local-region-exconf-rmq-kube/DPFM_API_Output_Formatter"
	"encoding/json"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type ExistenceConf struct {
	ctx context.Context
	db  *database.Mysql
	l   *logger.Logger
}

func NewExistenceConf(ctx context.Context, db *database.Mysql, l *logger.Logger) *ExistenceConf {
	return &ExistenceConf{
		ctx: ctx,
		db:  db,
		l:   l,
	}
}

func (e *ExistenceConf) Conf(msg rabbitmq.RabbitmqMessage) interface{} {
	var ret interface{}
	ret = map[string]interface{}{
		"ExistenceConf": false,
	}
	input := make(map[string]interface{})
	err := json.Unmarshal(msg.Raw(), &input)
	if err != nil {
		return ret
	}

	_, ok := input["LocalRegion"]
	if ok {
		input := &dpfm_api_input_reader.SDC{}
		err = json.Unmarshal(msg.Raw(), input)
		ret = e.confLocalRegion(input)
		goto endProcess
	}

	err = xerrors.Errorf("can not get exconf check target")
endProcess:
	if err != nil {
		e.l.Error(err)
	}
	return ret
}

func (e *ExistenceConf) confLocalRegion(input *dpfm_api_input_reader.SDC) *dpfm_api_output_formatter.LocalRegion {
	exconf := dpfm_api_output_formatter.LocalRegion{
		ExistenceConf: false,
	}
	if input.LocalRegion.LocalRegion == nil {
		return &exconf
	}
	if input.LocalRegion.Country == nil {
		return &exconf
	}
	exconf = dpfm_api_output_formatter.LocalRegion{
		LocalRegion:   *input.LocalRegion.LocalRegion,
		Country:       *input.LocalRegion.Country,
		ExistenceConf: false,
	}

	rows, err := e.db.Query(
		`SELECT LocalRegion
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_fin_inst_master_general_data 
		WHERE (LocalRegion, Country) = (?, ?);`, exconf.LocalRegion, exconf.Country,
	)
	if err != nil {
		e.l.Error(err)
		return &exconf
	}
	defer rows.Close()

	exconf.ExistenceConf = rows.Next()
	return &exconf
}
