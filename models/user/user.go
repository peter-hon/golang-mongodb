package user

import (
	mongocore "bitbucket.org/chaatzltd/statsforchaatz/core/mongo"
	"bitbucket.org/chaatzltd/statsforchaatz/models/mongo"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type UserData struct {
	mongo.Model `bson:",inline"`
	UserName       string   `bson:"username,omitempty"`
	Password       string   `bson:"password,omitempty"`
	Channel        []string `bson:"channel,omitempty"`
	PresentData    []string `bson:"present,omitempty"`
	Offset         int      `bson:"offset,omitempty"`
	DefaultChannel string   `bson:"defaultchan,omitempty"`
	HomePageType   string   `bson:"homepagetype,omitempty"`
	DataType       string   `bson:"data_type,omitempty"`
}

const collectionName = "user"

func New() *UserData {
	return &UserData {
		Model: *mongo.NewModel(),
	}
}

func WithC(s func(c *mgo.Collection) error) error {
	return mongocore.WithCollection(collectionName, s)
}

func Insert(userData *UserData) error {
	insert := func(c *mgo.Collection) error {
		return c.Insert(userData)
	}
	err := WithC(insert)
	if err != nil {
		return err
	}
	return nil
}

func GetUserData(userName string) (*UserData, error) {
	doc := UserData{}
	find := func(c *mgo.Collection) error {
		return c.Find(bson.M{"username": userName}).One(&doc)
	}
	err := WithC(find)
	if err != nil {
		return nil, err
	}
	return &doc, nil
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
