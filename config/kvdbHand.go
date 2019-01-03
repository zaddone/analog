package config

import(
	"github.com/boltdb/bolt"
	"time"
	"fmt"
)

func HandDBWithName(filename string,hand func(db *bolt.DB)error)error{

	//KvDB, err := bolt.Open(filename, 0600, &bolt.Options{Timeout: 5 * time.Second})
	KvDB, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		return err
	}
	err = hand(KvDB)
	KvDB.Close()
	return err
}

func UpdateKvDBWithName(filename string,bucket []byte,handle func(*bolt.Bucket) error ) error {
	return HandDBWithName(filename,func(db *bolt.DB)error{
		return db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(bucket)
			if err != nil {
				return err
			}
			return handle(b)
		})
	})
}

func ViewKvDBWithName(filename string,bucket []byte,handle func(*bolt.Bucket)error) error {
	return HandDBWithName(filename,func(db *bolt.DB)error{
		return db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucket)
			if b == nil {
				//return fmt.Errorf("%s is nil",string(bucket))
				return nil
			}
			return handle(b)
		})
	})
}


func HandDB(hand func(db *bolt.DB)error)error{
	KvDB, err := bolt.Open(Conf.KvDbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	err = hand(KvDB)
	KvDB.Close()
	return err
}
func ViewKvDB(bucket []byte,handle func(*bolt.Bucket)error) error {
	return HandDB(func(db *bolt.DB)error{
		return db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucket)
			if b== nil {
				return fmt.Errorf("%s is nil",string(bucket))
			}
			return handle(b)
		})
	})
}
func UpdateKvDB(bucket []byte,handle func(*bolt.Bucket) error ) error {
	return HandDB(func(db *bolt.DB)error{
		return db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(bucket)
			if err != nil {
				return err
			}
			return handle(b)
		})
	})
}

