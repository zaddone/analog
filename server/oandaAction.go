package server
import(
	"encoding/json"
	//"bytes"
	"io"
	"fmt"
	"net/url"
)

func HandleTrades(tp,sl,id string,accid int) (*TradesOrdersRequest,error) {
	path := AccountsMap[accid].GetAccountPath() + "/trades/"+id + "/orders"
	da, err := json.Marshal(map[string]interface{}{"takeProfit":&TakeProfitDetails{Price:tp},"stopLoss":&StopLossDetails{Price:sl}})
	if err != nil {
		panic(err)
	}
	var tr TradesOrdersRequest
	err = ClientIn(path,da,"PUT",200,func(body io.Reader)(er error){
		er = json.NewDecoder(body).Decode(&tr)
		if er != nil {
			panic(er)
		}
		return er
	})
	return &tr,err

}
//func ReadPosition(InsName string,accid int) 
func ClosePosition(InsName string,longUnits string,accid int) (mr PositionResponses, err error) {

	path := AccountsMap[accid].GetAccountPath()
	path += "/positions/" + InsName + "/close"

	val := make(map[string]string)
	//val["longUnits"] = "ALL"
	val["longUnits"] = longUnits
	da, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	err = ClientIn(path,da,"PUT",200,func(body io.Reader)(er error){
		er = json.NewDecoder(body).Decode(&mr)
		if er != nil {
			panic(er)
		}
		return er
	})
	if err != nil {
		return mr, err
	}
	return mr,err

}

func HandleOrder(InsName string,unit int, dif , Tp, Sl string,accid int) (mr OrderResponse, err error) {

	//path := GetNowAccount().GetAccountPath()
	path := AccountsMap[accid].GetAccountPath()
	path += "/orders"
	//Val := make(map[string]*MarketOrderRequest)
	order := NewMarketOrderRequest(InsName)
	//order.Init()
	order.SetUnits(unit)
	if dif != "" {
		order.SetTrailingStopLossDetails(dif)
	}
	if Sl != "" {
		order.SetStopLossDetails(Sl)
	}

	if Tp != "" {
		order.SetTakeProfitDetails(Tp)
	}
	//Val["order"] = order

	da, err := json.Marshal(map[string]*MarketOrderRequest{"order":order})
	if err != nil {
		panic(err)
	}
	err = ClientIn(path,da,"POST",201,func(body io.Reader)(er error){
		er = json.NewDecoder(body).Decode(&mr)
		if er != nil {
			panic(er)
		}
		return er
	})
	//err = ClientPost(path, bytes.NewReader(da), &mr)
	return mr, err

}

func GetTransactionSinceid(accid int,orderid int,handle func(*TransactionSinceidRes)) {

	uv := url.Values{}
	uv.Add("id",fmt.Sprintf("%d",accid))
	path := fmt.Sprintf("%s/transaction/sinceid?%s", AccountsMap[accid].GetAccountPath(),uv.Encode())
	var da TransactionSinceidRes
	err :=  ClientDo(path,func(body io.Reader)(er error){
		er =  json.NewDecoder(body).Decode(&da)
		if er != nil {
			panic(er)
			return er
		}
		handle(&da)
		return
	})
	if err != nil {
		panic(err)
	}

}
func GetTransaction(accid int,orderid int,handle func(*TransactionSinceidRes)) {

	path :=fmt.Sprintf("%s/transaction/%d", AccountsMap[accid].GetAccountPath(),orderid)
	var da TransactionSinceidRes
	err :=  ClientDo(path,func(body io.Reader)(er error){
		er =  json.NewDecoder(body).Decode(&da)
		if er != nil {
			panic(er)
			return er
		}
		handle(&da)
		return
	})
	if err != nil {
		panic(err)
	}

}
