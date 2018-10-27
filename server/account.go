package server
import(

	"encoding/json"
	"database/sql"
	"io"
)

type Account struct {
	Id          string                 `json:"id"`
	//Tags       []string               `json:"tags"`
	Mt4AccountID int		   `json:"mt4AccountID"`
	Instruments map[string]*Instrument
	InstrumentsId map[int]*Instrument
	_id  int64
}
func (self *Account) GetTemplateDataIns() interface{} {
	le :=len(self.Instruments)
	insList := make([]interface{},le)
	i :=0
	for _,ins := range self.Instruments {
		insList[i] = ins.GetTemplateData()
		i++
	}
	return insList
}
func (self *Account) GetTemplateData(index int) map[string]interface{} {
	return map[string]interface{}{
		"index":index,
		"id":self.Id,
		"uid":self._id}
}
func (self *Account)loadInstruments(db *sql.DB){
	self.Instruments = make(map[string]*Instrument)
	self.InstrumentsId = make(map[int]*Instrument)
	row,err := db.Query(`SELECT 
		id,
		displayPrecision,
		marginRate,
		maximumOrderUnits,
		maximumPositionSize,
		maximumTrailingStopDistance,
		minimumTradeSize,
		minimumTrailingStopDistance,
		name,
		pipLocation,
		tradeUnitsPrecision,
		type
		FROM instruments WHERE uid = ?`,self._id)
	for row.Next(){
		ins := Instrument{}
		err = row.Scan(
			&ins.id,
			&ins.DisplayPrecision,
			&ins.MarginRate,
			&ins.MaximumOrderUnits,
			&ins.MaximumPositionSize,
			&ins.MaximumTrailingStopDistance,
			&ins.MinimumTradeSize,
			&ins.MinimumTrailingStopDistance,
			&ins.Name,
			&ins.PipLocation,
			&ins.TradeUnitsPrecision,
			&ins.Type)
		if err != nil {
			panic(err)
		}
		ins.Uid = int(self._id)
		self.Instruments[ins.Name] = &ins
		self.InstrumentsId[ins.GetId()] = &ins
	}
	row.Close()
}
func (self *Account)GetStreamAccountPath() string {
	return StreamHost + "/accounts/" + self.Id
}
func (self *Account)GetAccountPath() string {
	return Host + "/accounts/" + self.Id
}
func (self *Account)downInstruments(db *sql.DB){
	err := ClientDo(self.GetAccountPath()+"/instruments",func(body io.Reader)(err error){
		//da := &InsResponse{}
		da := make(map[string]interface{})
		err =  json.NewDecoder(body).Decode(&da)
		if err != nil {
			return err
		}

		ins := da["instruments"].([]interface{})

		self.Instruments = make(map[string]*Instrument)
		for _,n := range ins {
			in := &Instrument{}
			in.load(n.(map[string]interface{}))
			in.Uid = int(self._id)
			res := StructSaveForDB(db,"instruments",in)
			in_id ,err := res.LastInsertId()
			if err != nil {
				panic(err)
			}
			in.id = int(in_id)
			self.Instruments[in.Name] = in
		}
		return nil

	})
	if err != nil {
		panic(err)
	}
}
