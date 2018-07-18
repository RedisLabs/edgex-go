package clients

//go:generate msgp

type RedisReading struct {
	Id       string `msg:"id"`
	Pushed   int64  `msg:"pushed"`  // When the data was pushed out of EdgeX (0 - not pushed yet)
	Created  int64  `msg:"created"` // When the reading was created
	Origin   int64  `msg:"origin"`
	Modified int64  `msg:"modified"`
	Device   string `msg:"device"`
	Name     string `msg:"name"`
	Value    string `msg:"value"` // Device sensor data value
}

type RedisEvent struct {
	ID       string         `msg:"id"`
	Pushed   int64          `msg:"pushed"`
	Device   string         `msg:"device"` // Device identifier (name or id)
	Created  int64          `msg:"created"`
	Modified int64          `msg:"modified"`
	Origin   int64          `msg:"origin"`
	Event    string         `msg:"event"` // Schedule event identifier
	Readings []RedisReading `msg:"-"`
}

type RedisValueDescriptor struct {
	Id           string      `msg:"id"`
	Created      int64       `msg:"created"`
	Description  string      `msg:"description"`
	Modified     int64       `msg:"modified"`
	Origin       int64       `msg:"origin"`
	Name         string      `msg:"name"`
	Min          interface{} `msg:"min"`
	Max          interface{} `msg:"max"`
	DefaultValue interface{} `msg:"defaultvalue"`
	Type         string      `msg:"type"`
	UomLabel     string      `msg:"uomlabel"`
	Formatting   string      `msg:"formatting"`
	Labels       []string    `msg:"labels"`
}
