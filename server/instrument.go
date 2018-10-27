package server
import(
	"strconv"
	"fmt"
	"math"
	//"sync"
)
type Instrument struct {
	id int
	Uid int `json:"uid"`
	Name string `json:"name"`

	DisplayPrecision float64 `json:"displayPrecision"`
	MarginRate       float64 `json:"marginRate"`
	MaximumOrderUnits           int `json:"maximumOrderUnits"`
	MaximumPositionSize         int `json:"maximumPositionSize"`
	MaximumTrailingStopDistance float64 `json:"maximumTrailingStopDistance"`

	MinimumTradeSize            int `json:"minimumTradeSize"`
	MinimumTrailingStopDistance float64 `json:"minimumTrailingStopDistance"`

	PipLocation         float64 `json:"pipLocation"`
	TradeUnitsPrecision float64 `json:"tradeUnitsPrecision"`
	Type                string `json:"type"`

}


func (self *Instrument) GetTemplateData() map[string]interface{} {
	return map[string]interface{}{
		"id":self.id,
		"account":AccountsMap[self.Uid].GetTemplateData(0),
		//"uid":self.Uid,
		"name":self.Name,
		"type":self.Type,
		"displayPrecision":self.DisplayPrecision,
		"minimumTrailingStopDistance":self.MinimumTrailingStopDistance	}

}
func (self *Instrument) GetId() int {
	return self.id
}
func (self *Instrument) load(tmp map[string]interface{}) (err error) {
	self.Name = tmp["name"].(string)
	self.PipLocation = tmp["pipLocation"].(float64)
	self.TradeUnitsPrecision = tmp["tradeUnitsPrecision"].(float64)
	self.Type = tmp["type"].(string)
	self.DisplayPrecision = tmp["displayPrecision"].(float64)

	self.MarginRate, err = strconv.ParseFloat(tmp["marginRate"].(string), 64)
	if err != nil {
		return err
	}
	self.MaximumOrderUnits, err = strconv.Atoi(tmp["maximumOrderUnits"].(string))
	if err != nil {
		return err
	}
	self.MaximumPositionSize, err = strconv.Atoi(tmp["maximumPositionSize"].(string))
	if err != nil {
		return err
	}
	self.MaximumTrailingStopDistance, err = strconv.ParseFloat(tmp["maximumTrailingStopDistance"].(string), 64)
	if err != nil {
		return err
	}
	self.MinimumTradeSize, err = strconv.Atoi(tmp["minimumTradeSize"].(string))
	if err != nil {
		return err
	}
	self.MinimumTrailingStopDistance, err = strconv.ParseFloat(tmp["minimumTrailingStopDistance"].(string), 64)
	if err != nil {
		return err
	}

	return nil
}
func (self *Instrument) StandardPrice(pr float64) string {

	_int,frac:= math.Modf(pr)
	frac  = math.Pow10(int(self.DisplayPrecision)) * frac
	return fmt.Sprintf("%d.%d", int(_int),int(frac))

}
