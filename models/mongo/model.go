package mongo

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

type Model struct {
	Id        bson.ObjectId `bson:"_id,omitempty" json:"id"`
	CreatedAt time.Time     `bson:"created_at,omitempty" json:"-"`
	UpdatedAt time.Time     `bson:"updated_at,omitempty" json:"-"`
}

func NewModel() *Model {
	return &Model{
		Id:        bson.NewObjectId(),
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}
