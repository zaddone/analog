package server
import(
	"log"
	"fmt"
	"time"
	"strings"
	"strconv"
	"math"
	"encoding/json"
)
type PositionRes struct {
	Position Position `json:"position"`
	lastTransactionID TransactionID `json:"lastTransactionID"`
}
type Position struct {
	Instrument InstrumentName `json:"instrument"`
	Pl AccountUnits `json:"pl"`
	UnrealizedPL AccountUnits `json:"unrealizedPL"`
	MarginUsed AccountUnits `json:"marginUsed"`
	ResettablePL AccountUnits `json:"resettablePL"`
	Financing AccountUnits `json:"financing"`
	Commission AccountUnits `json:"commission"`
	GuaranteedExecutionFees AccountUnits `json:"guaranteedExecutionFees"`
	Long PositionSide `json:"long"`
	Short PositionSide `json:"short"`
}
type PositionSide struct {
	Units DecimalNumber `json:"units"`
	AveragePrice PriceValue `json:"averagePrice"`
	TradeIDs []*TradeID `json:"tradeIDs"`
	Pl AccountUnits `json:"pl"`
	UnrealizedPL AccountUnits `json:"unrealizedPL"`
	ResettablePL AccountUnits `json:"resettablePL"`
	Financing AccountUnits `json:"financing"`
	GuaranteedExecutionFees AccountUnits `json:"guaranteedExecutionFees"`
}
type TradeState string
type TradeRes struct {
	Trade Trade `json:"trade"`
	TastTransactionID TransactionID `json:"lastTransactionID"`
}
type Trade struct {
	Id TradeID `json:"id"`
	Instrument InstrumentName `json:"instrument"`
	Price PriceValue `json:"price"`
	OpenTime DateTime `json:"openTime"`
	State TradeState `json:"state"`
	InitialUnits DecimalNumber `json:"initialUnits"`
	InitialMarginRequired AccountUnits `json:"initialMarginRequired"`
	CurrentUnits DecimalNumber `json:"currentUnits"`
	RealizedPL AccountUnits `json:"realizedPL"`
	UnrealizedPL AccountUnits `json:"unrealizedPL"`
	MarginUsed AccountUnits `json:"marginUsed"`
	AverageClosePrice PriceValue `json:"averageClosePrice"`
	ClosingTransactionIDs []TransactionID `json:"closingTransactionIDs"`
	Financing AccountUnits `json:"financing"`
	CloseTime DateTime `json:"closeTime"`
	ClientExtensions ClientExt `json:"clientExtensions"`
	TakeProfitOrder TakeProfitOrder `json:"takeProfitOrder"`
	StopLossOrder StopLossOrder `json:"stopLossOrder"`
	TrailingStopLossOrder TrailingStopLossOrder `json:"trailingStopLossOrder"`
}
type OrderState string
type OrderType string
type Order struct {
	Id OrderID `json:"id"`
	CreateTime DateTime `json:"createTime"`
	State OrderState `json:"state"`
	ClientExtensions ClientExt `json:"clientExtensions"`

}
type TakeProfitOrder struct {
	Order
	Type OrderType `json:"type"`
	TradeID TradeID `json:"tradeID"`
	ClientTradeID ClientID `json:"ClientTradeID"`
	Price PriceValue `json:"price"`
	TimeInForce TimeInForce `json:"timeInForce"`
	GtdTime DateTime `json:"gtdTime"`
	TriggerCondition OrderTriggerCondition `json:"triggerCondition"`
	FillingTransactionID TransactionID `json:"fillingTransactionID"`
	FilledTime DateTime `json:"filledTime"`
	TradeOpenedID TradeID `json:"tradeOpenedID"`
	TradeReducedID TradeID `json:"tradeReducedID"`
	TradeClosedIDs []TradeID `json:"TradeClosedIDs"`
	CancellingTransactionID TransactionID `json:"cancellingTransactionID"`
	CancelledTime DateTime `json:"cancelledTime"`
	ReplacesOrderID OrderID `json:"replacesOrderID"`
	ReplacedByOrderID OrderID `json:"replacedByOrderID"`
}
type StopLossOrder struct{
	Order
	Type OrderType `json:"type"`
	GuaranteedExecutionPremium DecimalNumber `json:"guaranteedExecutionPremium"`
	TradeID TradeID `json:"tradeID"`
	ClientTradeID ClientID `json:"ClientTradeID"`
	Price PriceValue `json:"price"`
	Distance DecimalNumber `json:"distance"`
	TimeInForce TimeInForce `json:"timeInForce"`
	GtdTime DateTime `json:"gtdTime"`
	TriggerCondition OrderTriggerCondition `json:"triggerCondition"`
	Guaranteed bool `json:"guaranteed"`
	FillingTransactionID TransactionID `json:"fillingTransactionID"`
	FilledTime DateTime `json:"filledTime"`
	TradeOpenedID TradeID `json:"tradeOpenedID"`
	TradeReducedID TradeID `json:"tradeReducedID"`
	TradeClosedIDs []TradeID `json:"TradeClosedIDs"`
	CancellingTransactionID TransactionID `json:"cancellingTransactionID"`
	CancelledTime DateTime `json:"cancelledTime"`
	ReplacesOrderID OrderID `json:"replacesOrderID"`
	ReplacedByOrderID OrderID `json:"replacedByOrderID"`

}
type TrailingStopLossOrder struct {
	Order
	Type OrderType `json:"type"`
	TradeID TradeID `json:"tradeID"`
	ClientTradeID ClientID `json:"ClientTradeID"`
	Distance DecimalNumber `json:"distance"`
	TimeInForce TimeInForce `json:"timeInForce"`
	GtdTime DateTime `json:"gtdTime"`
	TriggerCondition OrderTriggerCondition `json:"triggerCondition"`
	TrailingStopValue PriceValue `json:"trailingStopValue"`
	FillingTransactionID TransactionID `json:"fillingTransactionID"`
	FilledTime DateTime `json:"filledTime"`
	TradeOpenedID TradeID `json:"tradeOpenedID"`
	TradeReducedID TradeID `json:"tradeReducedID"`
	TradeClosedIDs []TradeID `json:"TradeClosedIDs"`
	CancellingTransactionID TransactionID `json:"cancellingTransactionID"`
	CancelledTime DateTime `json:"cancelledTime"`
	ReplacesOrderID OrderID `json:"replacesOrderID"`
	ReplacedByOrderID OrderID `json:"replacedByOrderID"`
}

type TradesOrdersRequest struct {
	LastTransactionID TransactionID `json:"lastTransactionID"`
	RelatedTransactionIDs []TransactionID `json:"relatedTransactionIDs"`
	StopLossOrderTransaction StopLossOrderTransaction `json:"stopLossOrderTransaction"`
	TakeProfitOrderTransaction TakeProfitOrderTransaction `json:"takeProfitOrderTransaction"`
}
type PricesResponses struct {
	Prices []*Price `json:"prices"`
	HomeConversions []*HomeConversions `json:"HomeConversions"`
	Time DateTime `json:"time"`
}
type Currency string
type HomeConversions struct {
	Currency Currency `json:"currency"`
	AccountGain DecimalNumber `json:"accountGain"`
	AccountLoss DecimalNumber `json:"accountLoss"`
	PositionValue DecimalNumber `json:"positionValue"`
}
type UnitsAvailableDetails struct {
	Long DecimalNumber `json:"long"`
	Short DecimalNumber `json:"short"`
}
type UnitsAvailable struct {
	Default UnitsAvailableDetails `json:"default"`
	ReduceFirst UnitsAvailableDetails `json:"reduceFirst"`
	ReduceOnly UnitsAvailableDetails `json:"reduceOnly"`
	OpenOnly UnitsAvailableDetails `json:"openOnly"`
}
//type DecimalNumber string
type QuoteHomeConversionFactors struct {
	PositiveUnits DecimalNumber `json:"positiveUnits"`
	NegativeUnits DecimalNumber `json:"negativeUnits"`
}
type Price struct {
	Type string `json:"type"`
	Instrument InstrumentName `json:"instrument"`
	Time DateTime `json:"time"`
	Status string `json:"status"`
	Tradeable bool `json:"tradeable"`
	Bids []*PriceBucket `json:"bids"`
	Asks []*PriceBucket `json:"asks"`
	CloseoutBid PriceValue `json:"closeoutBid"`
	CloseoutAsk PriceValue `json:"closeoutAsk"`
	QuoteHomeConversionFactors QuoteHomeConversionFactors `json:"quoteHomeConversionFactors"`
	UnitsAvailable UnitsAvailable `json:"unitsAvailable"`
}
func (self *Price) GetDiff() float64 {
	return math.Abs(self.CloseoutAsk.GetPrice() - self.CloseoutBid.GetPrice())
}
func (self *Price) GetPriceValue() float64 {
	return (self.CloseoutAsk.GetPrice() + self.CloseoutBid.GetPrice())*0.5
}
func (self *Price) ShowMsg() map[string]interface{} {

	return map[string]interface{}{"time":self.Time.TimeU(),"bid":self.CloseoutBid,"ask":self.CloseoutAsk}

}
type TradeID string
type TradeOpen struct {
	TradeID          TradeID       `json:"tradeID"`
	Units            DecimalNumber `json:"units"`
	ClientExtensions ClientExt     `json:"clientExtensions"`
}

type MarketOrderDelayedTradeClose struct {
	TradeID             TradeID       `json:"tradeID"`
	ClientTradeID       TradeID       `json:"clientTradeID"`
	SourceTransactionID TransactionID `json:"sourceTransactionID"`
}

type MarketOrderMarginCloseout struct {
	Reason string `json:"reason"`
}

type MarketOrderPositionCloseout struct {
	Instrument InstrumentName `json:"instrument"`
	Units      string         `json:"units"`
}
type MarketOrderTradeClose struct {
	TradeID       TradeID `json:"tradeID"`
	ClientTradeID string  `json:"clientTradeID"`
	Units         string  `json:"units"`
}
type PositionResponses struct {

	LongOrderCreateTransaction MarketOrderTransaction `json:"longOrderCreateTransaction"`
	LongOrderFillTransaction   OrderFillTransaction   `json:"LongOrderFillTransaction"`
	LongOrderCancelTransaction OrderCancelTransaction `json:"LongOrderCancelTransaction"`

	ShortOrderCreateTransaction MarketOrderTransaction `json:"shortOrderCreateTransaction"`
	ShortOrderFillTransaction   OrderFillTransaction   `json:"shortOrderFillTransaction"`
	ShortOrderCancelTransaction OrderCancelTransaction `json:"shortOrderCancelTransaction"`

	RelatedTransactionIDs []TransactionID `json:"relatedTransactionIDs"`
	LastTransactionID     TransactionID   `json:"lastTransactionID"`
}

type TransactionRejectReason string
type TransactionType string
type TransactionID string
type AccountFinancingMode string
type ClientID string
type TimeInForce string
type OrderTriggerCondition string

type Type struct {
	Type              TransactionType `json:"type"`
}

type TransactionsRequest struct {
	LastTransactionID TransactionID `json:"lastTransactionID"`
	Transactions []interface{}  `json:"transactions"`
	//Transactions [][]byte  `json:"transactions"`
}

type OpenTradeFinancing struct {
	TradeID		TradeID `json:"tradeID"`
	Financing	AccountUnits    `json:"financing"`
}

type TransactionHeartBeat struct {
	Type			TransactionType `json:"type"`
	LastTransactionID       TransactionID	`json:"lastTransactionID"`
	Time     DateTime      `json:"time"`
}
type Transaction struct {
	Id        TransactionID `json:"id"`
	Time      DateTime      `json:"time"`
	UserID    int           `json:"userID"`
	AccountID AccountID     `json:"accountID"`
	BatchID   TransactionID `json:"batchID"`
	RequestID RequestID     `json:"requestID"`
}


type CreateTransaction struct {

	Transaction
	Type        TransactionType `json:"type"`
	DivisionID  int `json:"divisionID"`
	SiteID	int `json:"siteID"`
	AccountUserID int `json:"accountUserID"`
	AccountNumber int `json:"accountNumber"`
	HomeCurrency Currency `json:"homeCurrency"`

}
type OrderCancelRejectTransaction struct {

	Transaction
	Type        TransactionType `json:"type"`
	OrderID		OrderID `json:"orderID"`
	ClientOrderID OrderID `json:"clientOrderID"`
	RejectReason TransactionRejectReason `json:"rejectReason"`

}

type ClientConfigureTransaction struct {

	Transaction
	Type        TransactionType `json:"type"`
	Alias	string `json:"alias"`
	MarginRate DecimalNumber `json:"marginRate"`

}

type TransferFundsTransaction struct {

	Transaction
	Type        TransactionType `json:"type"`
	Amount		AccountUnits `json:"amount"`
	FundingReason	string `json:"fundingReason"`
	Comment		string `json:"comment"`
	AccountBalance AccountUnits `json:"accountBalance"`

}

type TrailingStopLossOrderTransaction struct {

	Transaction
	Type              TransactionType `json:"type"`
	TradeID		TradeID `json:"tradeID"`
	ClientTradeID	ClientID `json:"clientTradeID"`
	Distance	DecimalNumber `json:"Distance"`
	TimeInForce  TimeInForce `json:"timeInForce"`
	GtdTime DateTime `json:"gtdTime"`
	TriggerCondition OrderTriggerCondition `json:"triggerCondition"`
	Reason            string          `json:"reason"`
	ClientExtensions       ClientExt  `json:"clientExtensions"`
	OrderFillTransactionID TransactionID `json:"orderFillTransactionID"`
	ReplacesOrderID OrderID `json:"replacesOrderID"`
	CancellingTransactionID TransactionID `json:"cancellingTransactionID"`

}


type MarketOrderRejectTransaction struct {
	Transaction
	Type              TransactionType `json:"type"`
	Instrument	InstrumentName  `json:"instrument"`
	Units          DecimalNumber   `json:"units"`
	TimeInForce	TimeInForce `json:"timeInForce"`
	PriceBount	PriceValue `json:"priceBount"`
	PositionFill   string      `json:"positionFill"`
	TradeClose   MarketOrderTradeClose `json:"tradeClose"`
	LongPositionCloseout   MarketOrderPositionCloseout  `json:"longPositionCloseout"`
	ShortPositionCloseout  MarketOrderPositionCloseout  `json:"shortPositionCloseout"`
	MarginCloseout         MarketOrderMarginCloseout    `json:"marginCloseout"`
	DelayedTradeClose      MarketOrderDelayedTradeClose `json:"delayedTradeClose"`
	Reason                 string                       `json:"reason"`
	ClientExtensions       ClientExt                    `json:"clientExtensions"`
	TakeProfitOnFill       TakeProfitDetails            `json:"takeProfitOnFill"`
	StopLossOnFill         StopLossDetails              `json:"stopLossOnFill"`
	TrailingStopLossOnFill TrailingStopLossDetails      `json:"trailingStopLossOnFill"`
	TradeClientExtensions  ClientExt                    `json:"tradeClientExtensions"`
	RejectReason TransactionRejectReason	`json:"rejectReason"`

}

type StopLossOrderTransaction struct {
	Transaction
	Type              TransactionType `json:"type"`
	TradeID		TradeID `json:"tradeID"`
	ClientTradeID	ClientID `json:"clientTradeID"`
	Price		PriceValue      `json:"price"`
	Distance	DecimalNumber `json:"Distance"`
	TimeInForce  TimeInForce `json:"timeInForce"`
	GtdTime DateTime `json:"gtdTime"`
	TriggerCondition OrderTriggerCondition `json:"triggerCondition"`
	Guaranteed bool `json:"guaranteed"`
	GuaranteedExecutionPremium DecimalNumber `json:"guaranteedExecutionPremium"`
	Reason            string          `json:"reason"`
	ClientExtensions       ClientExt  `json:"clientExtensions"`
	OrderFillTransactionID TransactionID `json:"orderFillTransactionID"`
	ReplacesOrderID OrderID `json:"replacesOrderID"`
	CancellingTransactionID TransactionID `json:"cancellingTransactionID"`

}
type TakeProfitOrderTransaction struct {
	Transaction
	Type              TransactionType `json:"type"`
	TradeID		TradeID `json:"tradeID"`
	ClientTradeID	ClientID `json:"clientTradeID"`
	Price          PriceValue      `json:"price"`
	TimeInForce  TimeInForce `json:"timeInForce"`
	GtdTime DateTime `json:"gtdTime"`
	TriggerCondition OrderTriggerCondition `json:"triggerCondition"`
	Reason            string          `json:"reason"`
	ClientExtensions       ClientExt  `json:"clientExtensions"`
	OrderFillTransactionID TransactionID `json:"orderFillTransactionID"`
	ReplacesOrderID OrderID `json:"replacesOrderID"`
	CancellingTransactionID TransactionID `json:"cancellingTransactionID"`

}
type PositionFinancing struct {

	Instrument	InstrumentName  `json:"instrument"`
	Financing	AccountUnits    `json:"financing"`
	OpenTradeFinancings []OpenTradeFinancing `json:"openTradeFinancings"`

}
type DailyFinancingTransaction struct {

	Transaction
	Type              TransactionType `json:"type"`
	Financing	  AccountUnits    `json:"financing"`
	AccountBalance		AccountUnits    `json:"accountBalance"`
	AccountFinancingMode AccountFinancingMode `json:"accountFinancingMode"`
	PositionFinancings []PositionFinancing `json:"positionFinancings"`

}
type OrderCancelTransaction struct {
	Transaction
	Type              TransactionType `json:"type"`
	OrderID           OrderID         `json:"orderID"`
	ClientOrderID     OrderID         `json:"clientOrderID"`
	Reason            string          `json:"reason"`
	ReplacedByOrderID OrderID         `json:"replacedByOrderID"`
}
type OrderFillTransaction struct {
	Transaction
	Type           TransactionType `json:"type"`
	OrderID        OrderID         `json:"orderID"`
	ClientOrderID  OrderID         `json:"clientOrderID"`
	Instrument     InstrumentName  `json:"instrument"`
	Units          DecimalNumber   `json:"units"`
	Price          PriceValue      `json:"price"`
	FullPrice      ClientPrice     `json:"fullPrice"`
	Reason         string          `json:"reason"`
	Pl             AccountUnits    `json:"pl"`
	Financing      AccountUnits    `json:"financing"`
	Commission     AccountUnits    `json:"commission"`
	AccountBalance AccountUnits    `json:"accountBalance"`
	TradeOpened    TradeO          `json:"tradeOpened"`
	TradesClosed   []TradeReduce   `json:"tradeOpened"`
	TradeReduced   TradeReduce     `json:"tradeReduced"`
}

func (self *OrderFillTransaction) GetReason() string{
	return string(self.Reason)
}
func (self *OrderFillTransaction) GetId() string{
	return string(self.Id)
}
func (self *OrderFillTransaction) GetType() string{
	return string(self.Type)
}
func (self *OrderFillTransaction) GetTime() time.Time {
	return self.Time.Time()
}

func (self *OrderFillTransaction) String() string {

	return fmt.Sprintf("%s %s %s %s %s %s %s %s %s",self.Id,self.BatchID,self.OrderID,self.Price,self.Units,self.Financing,self.Pl,self.AccountBalance,self.Reason)

}

type MarketOrderTransaction struct {
	Transaction
	Type                   string                       `json:"type"`
	Instrument             InstrumentName               `json:"Instrument"`
	Units                  DecimalNumber                `json:"units"`
	TimeInForce            string                       `json:"timeInForce"`
	PriceBound             PriceValue                   `json:"priceBound"`
	PositionFill           string                       `json:"positionFill"`
	TradeClose             MarketOrderTradeClose        `json:"tradeClose"`
	LongPositionCloseout   MarketOrderPositionCloseout  `json:"longPositionCloseout"`
	ShortPositionCloseout  MarketOrderPositionCloseout  `json:"shortPositionCloseout"`
	MarginCloseout         MarketOrderMarginCloseout    `json:"marginCloseout"`
	DelayedTradeClose      MarketOrderDelayedTradeClose `json:"delayedTradeClose"`
	Reason                 string                       `json:"reason"`
	ClientExtensions       ClientExt                    `json:"clientExtensions"`
	TakeProfitOnFill       TakeProfitDetails            `json:"takeProfitOnFill"`
	StopLossOnFill         StopLossDetails              `json:"stopLossOnFill"`
	TrailingStopLossOnFill TrailingStopLossDetails      `json:"trailingStopLossOnFill"`
	TradeClientExtensions  ClientExt                    `json:"tradeClientExtensions"`
}
type PriceValue string
type DateTime string
func (self DateTime) Time() time.Time {
	da := strings.Split(string(self),".")[0]
	daint,err := strconv.Atoi(da)
	if err != nil {
		panic(err)
	}
	return time.Unix(int64(daint),0)
}
func (self DateTime) TimeU() int64 {
	da := strings.Split(string(self),".")[0]
	daint,err := strconv.Atoi(da)
	if err != nil {
		return 0
		//panic(err)
	}
	return int64(daint)

}
type AccountID string
type RequestID string
type OrderID string
type InstrumentName string
type DecimalNumber string
type AccountUnits string
func (self AccountUnits) GetFloat() float64 {
	v,err := strconv.ParseFloat(string(self),64)
	if err != nil {
		//panic(err)
		//log.Fatalln("priceValue",err)
		return 0
	}
	return v
}

type PriceBucket struct {
	Price     PriceValue `json:"price"`
	Liquidity interface{} `json:"liquidity"`
}
func (self PriceValue) GetPrice() float64 {
	v,err := strconv.ParseFloat(string(self),64)
	if err != nil {
		//panic(err)
		//log.Println("priceValue",err)
		return 0
	}
	return v
}
type ClientPrice struct {
	Bids        []PriceBucket `json:"bids"`
	Asks        []PriceBucket `json:"Asks"`
	CloseoutBid PriceValue    `json:"closeoutBid"`
	CloseoutAsk PriceValue    `json:"closeoutAsk"`
	Timestamp   DateTime      `json:"timestamp"`
}
type TradeO struct {
	TradeID string `json:"tradeID"`
	Units   string `json:"units"`
}

type TradeReduce struct {
	TradeID    string `json:"tradeID"`
	Units      string `json:"units"`
	RealizedPL string `json:"realizedPL"`
	Financing  string `json:"financing"`
}
func(self *TradeReduce) GetId() string {
	return self.TradeID
}
func(self *TradeReduce) GetPl() float64 {
	pl,err := strconv.ParseFloat(self.RealizedPL,64)
	if err != nil {
		panic(err)
	}
	return pl
}

type OrderResponse struct {

	OrderRejectTransaction Transaction     `json:"orderRejectTransaction"`
	RelatedTransactionIDs  []TransactionID `json:"relatedTransactionIDs"`

	OrderCreateTransaction  Transaction  `json:"orderCreateTransaction"`
	OrderFillTransaction    OrderFillTransaction   `json:"orderFillTransaction"`
	OrderCancelTransaction  OrderCancelTransaction `json:"orderCancelTransaction"`
	OrderReissueTransaction Transaction  `json:"orderReissueTransaction"`

	LastTransactionID TransactionID `json:"lastTransactionID"`
	ErrorCode         string        `json:"errorCode"`
	ErrorMessage      string        `json:"errorMessage"`

}

type ClientExt struct {
	Id      string `json:"id,omitempty"`
	Tag     string `json:"tag,omitempty"`
	Comment string `json:"comment,omitempty"`
}
type TakeProfitDetails struct {
	Price            string     `json:"price,omitempty"`
	TimeInForce      string     `json:"timeInForce,omitempty"`
	GtdTime          string     `json:"gtdTime,omitempty"`
	ClientExtensions *ClientExt `json:"clientExtensions,omitempty"`
}
type StopLossDetails struct {
	Price            string     `json:"price,omitempty"`
	TimeInForce      string     `json:"timeInForce,omitempty"`
	GtdTime          string     `json:"gtdTime,omitempty"`
	ClientExtensions *ClientExt `json:"clientExtensions,omitempty"`
	Guaranteed  bool `json:"guaranteed"`
}
type TrailingStopLossDetails struct {
	Distance         string     `json:"distance,omitempty"`
	TimeInForce      string     `json:"timeInForce,omitempty"`
	GtdTime          string     `json:"gtdTime,omitempty"`
	ClientExtensions *ClientExt `json:"clientExtensions,omitempty"`
}
type MarketOrderRequest struct {
	Type                   string                   `json:"type,omitempty"`
	Instrument             string                   `json:"instrument,omitempty"`
	Units                  string                   `json:"units,omitempty"`
	TimeInForce            string                   `json:"timeInForce,omitempty"`
	PriceBount             string                   `json:"priceBount,omitempty"`
	PositionFill           string                   `json:"positionFill,omitempty"`
	ClientExtensions       *ClientExt               `json:"clientExtensions,omitempty"`
	TakeProfitOnFill       *TakeProfitDetails       `json:"takeProfitOnFill,omitempty"`
	StopLossOnFill         *StopLossDetails         `json:"stopLossOnFill,omitempty"`
	TrailingStopLossOnFill *TrailingStopLossDetails `json:"trailingStopLossOnFill,omitempty"`
	TradeClientExtensions  *ClientExt               `json:"tradeClientExtensions,omitempty"`
}

type TransactionRes struct {
	Transaction *Transaction `json:"transaction"`
	LastTransactionID TransactionID `json:"lastTransactionID"`


}
type TransactionSinceidRes struct {
	Transactions []*OrderFillTransaction `json:"transactions"`
	LastTransactionID TransactionID `json:"lastTransactionID"`
}

func NewMarketOrderRequest(InsName string) *MarketOrderRequest {
	return &MarketOrderRequest{
		Type:"MARKET",
		Instrument : InsName,
		TimeInForce : "FOK",
		PositionFill : "DEFAULT",
		PriceBount : "2"}
}

func (self *MarketOrderRequest) Init(InsName string) {
	self.Type = "MARKET"
	self.Instrument = InsName
	//	self.Units = "100"
	//	self.Units = fmt.Sprintf("%d",int(math.Pow(10,Instr.DisplayPrecision)*Instr.MinimumTradeSize))
	self.TimeInForce = "FOK"
	self.PositionFill = "DEFAULT"
	self.PriceBount = "2"
}
func (self *MarketOrderRequest) SetStopLossDetails(price string) {
	self.StopLossOnFill = &StopLossDetails{
		Price : price	}
}
func (self *MarketOrderRequest) SetTakeProfitDetails(price string) {
	self.TakeProfitOnFill = &TakeProfitDetails{
		Price : price	}
}

func (self *MarketOrderRequest) SetTrailingStopLossDetails(dif string) {
	self.TrailingStopLossOnFill = & TrailingStopLossDetails{
		Distance : dif	}
}

func (self *MarketOrderRequest) SetUnits(units int) {
	self.Units = fmt.Sprintf("%d", units)
}
func NewTransaction (tr TransactionType) (transaction interface{}) {

	switch tr {
		case "HEARTBEAT" :
			return new(TransactionHeartBeat)
		case "ORDER_FILL":
			return new(OrderFillTransaction)
		case "ORDER_CANCEL":
			return new(OrderCancelTransaction)
		case "MARKET_ORDER":
			return new(MarketOrderTransaction)
		case "DAILY_FINANCING":
			return new(DailyFinancingTransaction)
		case "TAKE_PROFIT_ORDER":
			return new(TakeProfitOrderTransaction)
		case "STOP_LOSS_ORDER":
			return new(StopLossOrderTransaction)
		case "TRAILING_STOP_LOSS_ORDER":
			return new(TrailingStopLossOrderTransaction)
		case "MARKET_ORDER_REJECT":
			return new(MarketOrderRejectTransaction)
		case "CREATE":
			return new(CreateTransaction)
		case "CLIENT_CONFIGURE":
			return new(ClientConfigureTransaction)
		case "TRANSFER_FUNDS":
			return new(TransferFundsTransaction)
		case "ORDER_CANCEL_REJECT":
			return new(OrderCancelRejectTransaction)
		default:
			panic(string(tr))
	}

}

func TransactionTypeCheck (da []byte) interface{} {

	var t Type
	err := json.Unmarshal(da,&t)
	if err != nil {
		panic(err)
	}
	tr := NewTransaction(t.Type)
	if tr == nil {
		log.Println(t.Type)

	}
	err = json.Unmarshal(da,&tr)
	if err != nil {
		panic(err)
	}
	return tr

}


