package register

import (
	"time"

	mongocore "bitbucket.org/chaatzltd/statsforchaatz/core/mongo"
	"bitbucket.org/chaatzltd/statsforchaatz/models/mongo"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const collectionName = "register_chaatz"

type Register struct {
	mongo.Model  `bson:",inline"`
	Channel      string  `bson:"channel"`
	CountryCode  string  `bson:"country_code"`
	DistinctId   string  `bson:"distinct_id"`
	Jid          string  `bson:"jid"`
	UserId       string  `bson:"user_id"`
	Event        string  `bson:"event"`
	Os           string  `bson:"os"`
	Uuid         string  `bson:"uuid"`
	RegisterTime string  `bson:"register_time"`
	NoOfProfiles float64 `bson:"no_of_profile_in_the _device"`
}

type ChannelInfo struct {
	Id    Channel `bson:"_id"`
	Count uint64  `bson:"count"`
}

type CountryInfo struct {
	Id    Country `bson:"_id"`
	Count uint64  `bson:"count"`
}

type OsInfo struct {
	Id    Os     `bson:"_id"`
	Count uint64 `bson:"count"`
}

type RegisterInfo struct {
	Id    Reg    `bson:"_id"`
	Count uint64 `bson:"count"`
}

type Reg struct {
	Channel      string    `bson:"channel"`
	UserId       string    `bson:"user_id"`
	Os           string    `bson:"os"`
	Country      string    `bson:"country_code"`
	RegisterTime time.Time `bson:"register_time"`
}

type Channel struct {
	Channel string `bson:"channel"`
	Year    uint64 `bson:"year"`
	Month   uint64 `bson:"month"`
	Day     uint64 `bson:"day"`
}

type Country struct {
	Country string `bson:"country_code"`
	Year    uint64 `bson:"year"`
	Month   uint64 `bson:"month"`
	Day     uint64 `bson:"day"`
}

type Os struct {
	Os    string `bson:"os"`
	Year  uint64 `bson:"year"`
	Month uint64 `bson:"month"`
	Day   uint64 `bson:"day"`
}

type TotalCount struct {
	Id    Country `bson:"_id"`
	Count uint64  `bson:"count"`
}

type sliceChannelInfo []ChannelInfo
type sliceCountryInfo []CountryInfo
type sliceOsInfo []OsInfo
type sliceRegister []RegisterInfo
type totalCount []TotalCount

func New() *Register {
	return &Register{
		Model: *mongo.NewModel(),
	}
}

func WithC(s func(c *mgo.Collection) error) error {
	return mongocore.WithCollection(collectionName, s)
}

func Insert(register *Register) error {
	insert := func(c *mgo.Collection) error {
		return c.Insert(register)
	}
	err := WithC(insert)
	if err != nil {
		return err
	}
	return nil
}

func FindByDate(date string) (int, error) {
	var count int
	find := func(c *mgo.Collection) error {
		var err error
		count, err = c.Find(bson.M{"created_at": date}).Count()
		return err
	}
	err := WithC(find)
	if err != nil {
		return 0, err
	}
	//fmt.Printf("count %v",count)
	return count, nil
}

func FindByGroupChannel(event string) (sliceChannelInfo, error) {
	var doc sliceChannelInfo
	t := time.Now().UTC()
	year := t.Year()
	month := t.Month()
	day := t.Day()
	groupChannel := func(c *mgo.Collection) error {
		//return c.Pipe([]bson.M{{"$group":bson.M{"_id":bson.M{"channel":"$channel"},"count":bson.M{"$sum":1}}}}).All(&doc)
		//return c.Find(bson.M{"created_at":bson.M{"$gte":fromDate}})
		return c.Pipe([]bson.M{
			{
				"$project": bson.M{
					"event":   "$event",
					"user_id": "$user_id",
					"channel": "$channel",
					"year": bson.M{
						"$year": "$created_at",
					},
					"month": bson.M{
						"$month": "$created_at",
					},
					"day": bson.M{
						"$dayOfMonth": "$created_at",
					},
				},
			},
			{
				"$match": bson.M{
					"year":  year,
					"month": month,
					"day":   day,
					"event": event,
				},
			},
			{
				"$group": bson.M{
					"_id": bson.M{
						"user_id": "$user_id",
						"channel": "$channel",
						"year":    "$year",
						"month":   "$month",
						"day":     "$day",
					},
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
			{
				"$group": bson.M{
					"_id": bson.M{
						"channel": "$_id.channel",
						"year":    "$_id.year",
						"month":   "$_id.month",
						"day":     "$_id.day",
					},
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
		}).All(&doc)
	}

	err := WithC(groupChannel)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func FindByGroupCountry(event string) (sliceCountryInfo, error) {
	var doc sliceCountryInfo
	t := time.Now().UTC()
	year := t.Year()
	month := t.Month()
	day := t.Day()
	groupCountry := func(c *mgo.Collection) error {
		//return c.Pipe([]bson.M{{"$group":bson.M{"_id":bson.M{"channel":"$channel"},"count":bson.M{"$sum":1}}}}).All(&doc)
		//return c.Find(bson.M{"created_at":bson.M{"$gte":fromDate}})
		return c.Pipe([]bson.M{
			{
				"$project": bson.M{
					"event":        "$event",
					"user_id":      "$user_id",
					"country_code": "$country_code",
					"year": bson.M{
						"$year": "$created_at",
					},
					"month": bson.M{
						"$month": "$created_at",
					},
					"day": bson.M{
						"$dayOfMonth": "$created_at",
					},
				},
			},
			{
				"$match": bson.M{
					"year":  year,
					"month": month,
					"day":   day,
					"event": event,
				},
			},
			{
				"$group": bson.M{
					"_id": bson.M{
						"user_id":      "$user_id",
						"country_code": "$country_code",
						"year":         "$year",
						"month":        "$month",
						"day":          "$day",
					},
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
			{
				"$group": bson.M{
					"_id": bson.M{
						"country_code": "$_id.country_code",
						"year":         "$_id.year",
						"month":        "$_id.month",
						"day":          "$_id.day",
					},
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
		}).All(&doc)
	}

	err := WithC(groupCountry)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func FindByGroupOs(event string) (sliceOsInfo, error) {
	var doc sliceOsInfo
	t := time.Now().UTC()
	year := t.Year()
	month := t.Month()
	day := t.Day()
	groupOs := func(c *mgo.Collection) error {
		//return c.Pipe([]bson.M{{"$group":bson.M{"_id":bson.M{"channel":"$channel"},"count":bson.M{"$sum":1}}}}).All(&doc)
		//return c.Find(bson.M{"created_at":bson.M{"$gte":fromDate}})
		return c.Pipe([]bson.M{
			{
				"$project": bson.M{
					"event":   "$event",
					"user_id": "$user_id",
					"os":      "$os",
					"year": bson.M{
						"$year": "$created_at"},
					"month": bson.M{
						"$month": "$created_at"},
					"day": bson.M{
						"$dayOfMonth": "$created_at",
					},
				},
			},
			{
				"$match": bson.M{
					"year":  year,
					"month": month,
					"day":   day,
					"event": event,
				},
			},
			{
				"$group": bson.M{
					"_id": bson.M{
						"user_id": "$user_id",
						"os":      "$os",
						"year":    "$year",
						"month":   "$month",
						"day":     "$day",
					},
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
			{
				"$group": bson.M{
					"_id": bson.M{
						"os":    "$_id.os",
						"year":  "$_id.year",
						"month": "$_id.month",
						"day":   "$_id.day",
					},
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
		}).All(&doc)
	}

	err := WithC(groupOs)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func FindByDateRangeAndChannel(startDate, endDate time.Time, event, channel string, offset int, limit int) (sliceRegister, error) {
	var doc sliceRegister
	groupOs := func(c *mgo.Collection) error {
		return c.Pipe([]bson.M{
			{
				"$project": bson.M{
					"event":         "$event",
					"user_id":       "$user_id",
					"os":            "$os",
					"register_time": "$register_time",
					"channel":       "$channel",
					"country_code":  "$country_code",
				},
			},
			{
				"$match": bson.M{
					"register_time": bson.M{
						"$gt": startDate,
						"$lt": endDate,
					},
					"event":   event,
					"channel": channel,
				},
			},
			{
				"$group": bson.M{
					"_id": bson.M{
						"user_id":       "$user_id",
						"os":            "$os",
						"channel":       "$channel",
						"register_time": "$register_time",
						"country_code":  "$country_code",
					},
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
			{
				"$sort": bson.M{
					"_id.register_time": -1,
				},
			},
			{
				"$skip": offset,
			},
			{
				"$limit": limit,
			},
		}).All(&doc)
	}

	err := WithC(groupOs)
	if err != nil {
		return nil, err
	}

	doc1 := doc.ToUTC()

	return doc1, nil
}

func (doc sliceRegister) ToUTC() sliceRegister {
	var doc1 sliceRegister
	for _, v := range doc {
		v.Id.RegisterTime = v.Id.RegisterTime.UTC()
		doc1 = append(doc1, v)
	}

	return doc1
}

func FindTotalCountByDateRangeAndChannel(startDate, endDate time.Time, event, channel string) (totalCount, error) {
	var doc totalCount
	groupOs := func(c *mgo.Collection) error {
		return c.Pipe([]bson.M{
			{
				"$project": bson.M{
					"event":         "$event",
					"user_id":       "$user_id",
					"os":            "$os",
					"register_time": "$register_time",
					"channel":       "$channel",
				},
			},
			{
				"$match": bson.M{
					"register_time": bson.M{
						"$gt": startDate,
						"$lt": endDate,
					},
					"event":   event,
					"channel": channel,
				},
			},
			{
				"$group": bson.M{
					"_id": bson.M{},
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
		}).All(&doc)
	}

	err := WithC(groupOs)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func FindTotalCountByEventAndDateAndChannel(startDate, endDate time.Time, event, channel string) (sliceRegister, error) {
	var doc sliceRegister
	groupOs := func(c *mgo.Collection) error {
		return c.Pipe([]bson.M{
			{
				"$project": bson.M{
					"event":         "$event",
					"user_id":       "$user_id",
					"os":            "$os",
					"register_time": "$register_time",
					"channel":       "$channel",
					"country_code":  "$country_code",
				},
			},
			{
				"$match": bson.M{
					"register_time": bson.M{
						"$gt": startDate,
						"$lt": endDate,
					},
					"event":   event,
					"channel": channel,
				},
			},
			{
				"$group": bson.M{
					"_id": bson.M{
						"user_id":       "$user_id",
						"os":            "$os",
						"channel":       "$channel",
						"register_time": "$register_time",
						"country_code":  "$country_code",
					},
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
			{
				"$sort": bson.M{
					"_id.register_time": -1,
				},
			},
		}).All(&doc)
	}

	err := WithC(groupOs)
	if err != nil {
		return nil, err
	}

	doc1 := doc.ToUTC()

	return doc1, nil
}

func FindByTotalCount() (sliceChannelInfo, error) {
	var doc sliceChannelInfo
	t := time.Now().UTC()
	year := t.Year()
	month := t.Month()
	day := t.Day()
	groupChannel := func(c *mgo.Collection) error {
		//return c.Pipe([]bson.M{{"$group":bson.M{"_id":bson.M{"channel":"$channel"},"count":bson.M{"$sum":1}}}}).All(&doc)
		//return c.Find(bson.M{"created_at":bson.M{"$gte":fromDate}})
		return c.Pipe([]bson.M{
			{
				"$project": bson.M{
					"channel": "$channel",
					"year": bson.M{
						"$year": "$created_at",
					},
					"month": bson.M{
						"$month": "$created_at",
					},
					"day": bson.M{
						"$dayOfMonth": "$created_at",
					},
				},
			},
			{
				"$match": bson.M{
					"year":  year,
					"month": month,
					"day":   day,
				},
			},
			{
				"$group": bson.M{
					"_id": nil,
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
		}).All(&doc)
	}

	err := WithC(groupChannel)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func FindByGroupCountryAndChannel(channel string) (sliceCountryInfo, error) {
	var doc sliceCountryInfo
	t := time.Now().UTC()
	year := t.Year()
	month := t.Month()
	day := t.Day()
	groupCountry := func(c *mgo.Collection) error {
		//return c.Pipe([]bson.M{{"$group":bson.M{"_id":bson.M{"channel":"$channel"},"count":bson.M{"$sum":1}}}}).All(&doc)
		//return c.Find(bson.M{"created_at":bson.M{"$gte":fromDate}})
		return c.Pipe([]bson.M{
			{
				"$project": bson.M{
					"country_code": "$country_code",
					"year": bson.M{
						"$year": "$created_at",
					},
					"month": bson.M{
						"$month": "$created_at",
					},
					"day": bson.M{
						"$dayOfMonth": "$created_at",
					},
					"channelSubstring": bson.M{
						"$substr": []interface{}{"$distinct_id", 0, 6},
					},
				},
			},

			{
				"$match": bson.M{
					"year":             year,
					"month":            month,
					"day":              day,
					"channelSubstring": channel,
				},
			},

			{
				"$group": bson.M{
					"_id": bson.M{
						"country_code": "$country_code",
						"year":         "$year",
						"month":        "$month",
						"day":          "$day",
					},
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
			{
				"$sort": bson.M{
					"count": -1,
				},
			},
		}).All(&doc)
	}

	err := WithC(groupCountry)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func Iter(query interface{}) *mgo.Iter {
	var iter *mgo.Iter
	find := func(c *mgo.Collection) error {
		iter = c.Find(query).Iter()
		return nil
	}
	_ = WithC(find)
	return iter
}
