package session

import (
	"crypto/rand"
	"encoding/base64"
	mgo "gopkg.in/mgo.v2"
	"io"

	mongocore "bitbucket.org/chaatzltd/statsforchaatz/core/mongo"
	"bitbucket.org/chaatzltd/statsforchaatz/models/mongo"

	"github.com/Sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
	"time"
)

const collectionName = "session"

type Session struct {
	mongo.Model `bson:",inline"`
	UserName    string `bson:"user_name,omitempty"`
	SessionID   string `bson:"session_id,omitempty"`
}

func New() *Session {
	return &Session{
		Model: *mongo.NewModel(),
	}
}

func GenerateSession(username string) string {
	log := logrus.WithFields(logrus.Fields{"module": "session", "module_method": "GenerateSession"})

	sid := makeStr()
	var usersession Session
	usersession.UserName = username
	usersession.SessionID = sid

	err := Insert(&usersession)
	if err != nil {
		log.WithField("err", err).Error("Failed to create session")
		return ""
	}

	return sid
}

func CheckSession(sid string) (string, bool) {
	session, err := FindBySid(sid)
	if err != nil {
		return "", false
	}

	if session.UserName != "" {
		UpdateSession(sid)
		return session.UserName, true
	} else {
		return "", false
	}
}

func Insert(session *Session) error {
	insert := func(c *mgo.Collection) error {
		return c.Insert(session)
	}
	err := WithC(insert)
	if err != nil {
		return err
	}
	return nil
}

func FindBySid(sid string) (*Session, error) {
	doc := Session{}
	find := func(c *mgo.Collection) error {
		return c.Find(bson.M{"session_id": sid}).One(&doc)
	}
	err := WithC(find)
	if err != nil {
		return nil, err
	}

	return &doc, nil
}

func UpdateSession(sid string) error {
	currentTime := time.Now()

	update := func(c *mgo.Collection) error {
		return c.Update(bson.M{"sid": sid}, bson.M{"CreatedAt": currentTime})
	}

	err := WithC(update)
	if err != nil {
		return err
	}

	return nil
}

func RemoveSession(sid string) error {
	update := func(c *mgo.Collection) error {
		return c.Remove(bson.M{"session_id": sid})
	}

	err := WithC(update)

	if err != nil {
		return err
	}

	return nil
}

func WithC(s func(c *mgo.Collection) error) error {
	return mongocore.WithCollection(collectionName, s)
}

func makeStr() string {
	b := make([]byte, 8)
	n, err := io.ReadFull(rand.Reader, b)
	if n != len(b) || err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}
