/*******************************************************************************
 * Copyright 2018 Redis Labs Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
package clients

import (
	"strconv"
	"time"

	"github.com/imdario/mergo"

	"github.com/edgexfoundry/edgex-go/core/domain/models"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/mgo.v2/bson"
)

var currentRedisClient *RedisClient

var getObjectsByRange = getObjectsByRangeLua
var getObjectsByRangeFilter = getObjectsByRangeFilterLua
var getObjectsByScore = getObjectsByScoreLua

// RedisClient represents a client
type RedisClient struct {
	Pool       *redis.Pool // Connections to Redis
	isEmbedded bool
}

// Return a pointer to the RedisClient
func newRedisClient(config DBConfiguration) (*RedisClient, error) {
	if currentRedisClient == nil {
		connectionString := config.Host + ":" + strconv.Itoa(config.Port)
		loggingClient.Info("INFO: Connecting to Redis at: " + connectionString)

		var proto, addr string

		opts := []redis.DialOption{
			redis.DialPassword(config.Password),
			redis.DialConnectTimeout(time.Duration(config.Timeout) * time.Millisecond),
		}

		dialFunc := func() (redis.Conn, error) {
			// TODO: add timeout and password from config
			conn, err := redis.Dial(
				proto, addr, opts...,
			)
			if err != nil {
				loggingClient.Error("Error dialing the Redis server: " + err.Error())
				return nil, err
			}
			return conn, nil
		}

		embedded := (config.Host == "")
		if embedded {
			// embedded
			proto = "eredis"
			addr = ""
		} else {
			if string(config.Host[0]) == "/" {
				// unix domain socket
				proto = "unix"
				addr = config.Host
			} else {
				// network connection
				proto = "tcp"
				addr = connectionString
			}
		}

		pool := &redis.Pool{
			MaxIdle:     10,
			IdleTimeout: 0,
			Dial:        dialFunc,
		}

		currentRedisClient = &RedisClient{Pool: pool, isEmbedded: (proto == "eredis")}
	}

	return currentRedisClient, nil
}

// CloseSession closes the connections to Redis
func (rc *RedisClient) CloseSession() {
	rc.Pool.Close()
}

// ******************************* EVENTS **********************************

// ********************** EVENT FUNCTIONS *******************************
// Return all the events
// Sort the events in descending order by ID
// UnexpectedError - failed to retrieve events from the database
func (rc *RedisClient) Events() (events []models.Event, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	objects, err := getObjectsByRange(conn, EVENTS_COLLECTION, 0, -1)
	if err != nil {
		if err != redis.ErrNil {
			return events, err
		}
	}

	events, err = eventsFromObjects(objects)
	if err != nil {
		return events, err
	}

	return events, nil
}

// Add a new event
// UnexpectedError - failed to add to database
// NoValueDescriptor - no existing value descriptor for a reading in the event
func (rc *RedisClient) AddEvent(e *models.Event) (id bson.ObjectId, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	err = addEvent(conn, e)
	if err != nil {
		return e.ID, err
	}

	id = e.ID
	return id, nil
}

// Update an event - do NOT update readings
// UnexpectedError - problem updating in database
// NotFound - no event with the ID was found
func (rc *RedisClient) UpdateEvent(e models.Event) (err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	id := e.ID.Hex()

	o, err := eventByID(conn, id)
	if err != nil {
		if err == redis.ErrNil {
			return ErrNotFound
		}
		return err
	}

	e.Modified = time.Now().UnixNano() / int64(time.Millisecond)
	err = mergo.Merge(&e, o)
	if err != nil {
		return err
	}

	err = deleteEvent(conn, id)
	if err != nil {
		return err
	}

	err = addEvent(conn, &e)
	if err != nil {
		return err
	}

	return nil
}

// Get an event by id
func (rc *RedisClient) EventById(id string) (event models.Event, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	event, err = eventByID(conn, id)
	if err != nil {
		if err == redis.ErrNil {
			return event, ErrNotFound
		}
		return event, err
	}

	return event, nil
}

// Get the number of events in Core Data
func (rc *RedisClient) EventCount() (count int, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	count, err = redis.Int(conn.Do("ZCARD", EVENTS_COLLECTION))
	if err != nil {
		return 0, err
	}

	return count, nil
}

// Get the number of events in Core Data for the device specified by id
func (rc *RedisClient) EventCountByDeviceId(id string) (count int, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	count, err = redis.Int(conn.Do("ZCARD", EVENTS_COLLECTION+":device:"+id))
	if err != nil {
		return 0, err
	}

	return count, nil
}

// Update an event by ID
// Set the pushed variable to the current time
// 404 - Event not found
// 503 - Unexpected problems
//UpdateEventById(id string) error

// Delete an event by ID and all of its readings
// 404 - Event not found
// 503 - Unexpected problems
func (rc *RedisClient) DeleteEventById(id string) (err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	err = deleteEvent(conn, id)
	if err != nil {
		if err == redis.ErrNil {
			return ErrNotFound
		}
		return err
	}

	return nil
}

// Get a list of events based on the device id and limit
func (rc *RedisClient) EventsForDeviceLimit(id string, limit int) (events []models.Event, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	objects, err := getObjectsByRange(conn, EVENTS_COLLECTION+":device:"+id, 0, limit-1)
	if err != nil {
		if err != redis.ErrNil {
			return events, err
		}
	}

	events, err = eventsFromObjects(objects)
	if err != nil {
		return events, err
	}

	return events, nil
}

// Get a list of events based on the device id
func (rc *RedisClient) EventsForDevice(id string) (events []models.Event, err error) {
	events, err = rc.EventsForDeviceLimit(id, 0)
	if err != nil {
		return nil, err
	}
	return events, nil
}

// Delete all of the events by the device id (and the readings)
//DeleteEventsByDeviceId(id string) error

// Return a list of events whos creation time is between startTime and endTime
// Limit the number of results by limit
func (rc *RedisClient) EventsByCreationTime(startTime, endTime int64, limit int) (events []models.Event, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	objects, err := getObjectsByScore(conn, EVENTS_COLLECTION+":created", startTime, endTime, limit)
	if err != nil {
		if err != redis.ErrNil {
			return events, err
		}
	}

	events, err = eventsFromObjects(objects)
	if err != nil {
		return events, err
	}

	return events, nil
}

// Return a list of readings for a device filtered by the value descriptor and limited by the limit
// The readings are linked to the device through an event
func (rc *RedisClient) ReadingsByDeviceAndValueDescriptor(deviceId, valueDescriptor string, limit int) (readings []models.Reading, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	if limit == 0 {
		return readings, nil
	}

	objects, err := getObjectsByRangeFilter(conn,
		READINGS_COLLECTION+":device:"+deviceId,
		READINGS_COLLECTION+":name:"+valueDescriptor,
		0, limit-1)
	if err != nil {
		return readings, err
	}

	readings, err = readingsFromObjects(objects)
	if err != nil {
		return readings, err
	}

	return readings, nil

}

// Remove all the events that are older than the given age
// Return the number of events removed
//RemoveEventByAge(age int64) (int, error)

// Get events that are older than a age
func (rc *RedisClient) EventsOlderThanAge(age int64) ([]models.Event, error) {
	expireDate := (time.Now().UnixNano() / int64(time.Millisecond)) - age

	return rc.EventsByCreationTime(0, expireDate, 0)
}

// Remove all the events that have been pushed
//func (dbc *DBClient) ScrubEvents()(int, error)

// Get events that have been pushed (pushed field is not 0)
func (rc *RedisClient) EventsPushed() (events []models.Event, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	objects, err := getObjectsByScore(conn, EVENTS_COLLECTION+":pushed", 1, -1, 0)
	if err != nil {
		return events, err
	}

	events, err = eventsFromObjects(objects)
	if err != nil {
		return events, err
	}

	return events, nil
}

// Delete all readings and events
func (rc *RedisClient) ScrubAllEvents() (err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	err = unlinkCollection(conn, EVENTS_COLLECTION)
	if err != nil {
		return err
	}

	err = unlinkCollection(conn, READINGS_COLLECTION)
	if err != nil {
		return err
	}

	return nil
}

// ********************* READING FUNCTIONS *************************
// Return a list of readings sorted by reading id
func (rc *RedisClient) Readings() (readings []models.Reading, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	objects, err := getObjectsByRange(conn, READINGS_COLLECTION, 0, -1)
	if err != nil {
		return readings, err
	}

	readings, err = readingsFromObjects(objects)
	if err != nil {
		return readings, err
	}

	return readings, nil
}

// Post a new reading
// Check if valuedescriptor exists in the database
func (rc *RedisClient) AddReading(r models.Reading) (id bson.ObjectId, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	err = addReading(conn, true, &r)
	if err != nil {
		return r.Id, err
	}

	id = r.Id
	return id, nil
}

// Update a reading
// 404 - reading cannot be found
// 409 - Value descriptor doesn't exist
// 503 - unknown issues
func (rc *RedisClient) UpdateReading(r models.Reading) error {
	conn := rc.Pool.Get()
	defer conn.Close()

	id := r.Id.Hex()

	o, err := readingByID(conn, id)
	if err != nil {
		if err == redis.ErrNil {
			return ErrNotFound
		}
		return err
	}

	r.Modified = time.Now().UnixNano() / int64(time.Millisecond)
	err = mergo.Merge(&r, o)
	if err != nil {
		return err
	}

	err = deleteReading(conn, id)
	if err != nil {
		return err
	}
	err = addReading(conn, true, &r)
	if err != nil {
		return err
	}

	return nil
}

// Get a reading by ID
func (rc *RedisClient) ReadingById(id string) (reading models.Reading, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	reading, err = readingByID(conn, id)
	if err != nil {
		if err == redis.ErrNil {
			return reading, ErrNotFound
		}
		return reading, err
	}

	return reading, nil
}

// Get the number of readings in core data
func (rc *RedisClient) ReadingCount() (int, error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	count, err := redis.Int(conn.Do("ZCARD", READINGS_COLLECTION))
	if err != nil {
		return 0, err
	}

	return count, nil
}

// Delete a reading by ID
// 404 - can't find the reading with the given id
func (rc *RedisClient) DeleteReadingById(id string) error {
	conn := rc.Pool.Get()
	defer conn.Close()

	err := deleteReading(conn, id)
	if err != nil {
		return err
	}

	return nil
}

// Return a list of readings for the given device (id or name)
// 404 - meta data checking enabled and can't find the device
// Sort the list of readings on creation date
func (rc *RedisClient) ReadingsByDevice(id string, limit int) (readings []models.Reading, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	objects, err := getObjectsByRange(conn, READINGS_COLLECTION+":device:"+id, 0, limit-1)
	if err != nil {
		if err != redis.ErrNil {
			return readings, err
		}
	}

	readings, err = readingsFromObjects(objects)
	if err != nil {
		return readings, err
	}

	return readings, nil
}

// Return a list of readings for the given value descriptor
// 413 - the number exceeds the current max limit
func (rc *RedisClient) ReadingsByValueDescriptor(name string, limit int) (readings []models.Reading, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	objects, err := getObjectsByRange(conn, READINGS_COLLECTION+":name:"+name, 0, limit-1)
	if err != nil {
		if err != redis.ErrNil {
			return readings, err
		}
	}

	readings, err = readingsFromObjects(objects)
	if err != nil {
		return readings, err
	}

	return readings, nil
}

// Return a list of readings whose name is in the list of value descriptor names
func (rc *RedisClient) ReadingsByValueDescriptorNames(names []string, limit int) (readings []models.Reading, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	if limit == 0 {
		return readings, nil
	}
	limit--

	for _, name := range names {
		objects, err := getObjectsByRange(conn, READINGS_COLLECTION+":name:"+name, 0, limit)
		if err != nil {
			if err != redis.ErrNil {
				return readings, err
			}
		}

		t, err := readingsFromObjects(objects)
		if err != nil {
			return readings, err
		}

		readings = append(readings, t...)

		limit -= len(objects)
		if limit < 0 {
			break
		}
	}

	return readings, nil
}

// Return a list of readings specified by the UOM label
//ReadingsByUomLabel(uomLabel string, limit int)([]models.Reading, error)

// Return a list of readings based on the label (value descriptor)
// 413 - limit exceeded
//ReadingsByLabel(label string, limit int) ([]models.Reading, error)

// Return a list of readings who's value descriptor has the type
//ReadingsByType(typeString string, limit int) ([]models.Reading, error)

// Return a list of readings whos created time is between the start and end times
func (rc *RedisClient) ReadingsByCreationTime(start, end int64, limit int) (readings []models.Reading, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	if limit == 0 {
		return readings, nil
	}

	objects, err := getObjectsByScore(conn, READINGS_COLLECTION+":created", start, end, limit)
	if err != nil {
		return readings, err
	}

	readings, err = readingsFromObjects(objects)
	if err != nil {
		return readings, err
	}

	return readings, nil
}

// ************************** VALUE DESCRIPTOR FUNCTIONS ***************************
// Add a value descriptor
// 409 - Formatting is bad or it is not unique
// 503 - Unexpected
// TODO: Check for valid printf formatting
func (rc *RedisClient) AddValueDescriptor(v models.ValueDescriptor) (id bson.ObjectId, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	err = addValue(conn, &v)
	if err != nil {
		return v.Id, err
	}

	id = v.Id
	return id, nil
}

// Return a list of all the value descriptors
// 513 Service Unavailable - database problems
func (rc *RedisClient) ValueDescriptors() (values []models.ValueDescriptor, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	objects, err := getObjectsByRange(conn, VALUE_DESCRIPTOR_COLLECTION, 0, -1)
	if err != nil {
		return values, err
	}

	values, err = valuesFromObjects(objects)
	if err != nil {
		return values, err
	}

	return values, nil
}

// Update a value descriptor
// First use the ID for identification, then the name
// TODO: Check for the valid printf formatting
// 404 not found if the value descriptor cannot be found by the identifiers
func (rc *RedisClient) UpdateValueDescriptor(v models.ValueDescriptor) error {
	conn := rc.Pool.Get()
	defer conn.Close()

	id := v.Id.Hex()
	o, err := valueByName(conn, v.Name)
	if err != nil && err != redis.ErrNil {
		return err
	}
	if err == nil && o.Id != v.Id {
		// IDs are different -> name not unique
		return ErrNotUnique
	}

	v.Modified = time.Now().UnixNano() / int64(time.Millisecond)
	err = mergo.Merge(&v, o)
	if err != nil {
		return err
	}

	err = deleteValue(conn, id)
	if err != nil {
		return err
	}

	err = addValue(conn, &v)
	if err != nil {
		return err
	}

	return nil

	return nil
}

// Delete a value descriptor based on the ID
func (rc *RedisClient) DeleteValueDescriptorById(id string) error {
	conn := rc.Pool.Get()
	defer conn.Close()

	err := deleteValue(conn, id)
	if err != nil {
		if err == redis.ErrNil {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// Return a value descriptor based on the name
func (rc *RedisClient) ValueDescriptorByName(name string) (value models.ValueDescriptor, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	value, err = valueByName(conn, name)
	if err != nil {
		if err == redis.ErrNil {
			return value, ErrNotFound
		}
		return value, err
	}

	return value, nil
}

// Return value descriptors based on the names
func (rc *RedisClient) ValueDescriptorsByName(names []string) (values []models.ValueDescriptor, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	for _, name := range names {
		value, err := valueByName(conn, name)
		if err != nil && err != redis.ErrNil {
			return nil, err
		}

		if err == nil {
			values = append(values, value)
		}
	}

	return values, nil
}

// Delete a valuedescriptor based on the name
//DeleteValueDescriptorByName(name string) error

// Return a value descriptor based on the id
func (rc *RedisClient) ValueDescriptorById(id string) (value models.ValueDescriptor, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	value, err = valueByID(conn, id)
	if err == redis.ErrNil {
		return value, ErrNotFound
	}
	if err != nil {
		return value, err
	}

	return value, nil
}

// Return value descriptors based on the unit of measure label
func (rc *RedisClient) ValueDescriptorsByUomLabel(uomLabel string) (values []models.ValueDescriptor, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	objects, err := getObjectsByRange(conn, VALUE_DESCRIPTOR_COLLECTION+":uomlabel:"+uomLabel, 0, -1)
	if err != nil {
		if err != redis.ErrNil {
			return values, err
		}
	}

	values, err = valuesFromObjects(objects)
	if err != nil {
		return values, err
	}

	return values, nil
}

// Return value descriptors based on the label
func (rc *RedisClient) ValueDescriptorsByLabel(label string) (values []models.ValueDescriptor, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	objects, err := getObjectsByRange(conn, VALUE_DESCRIPTOR_COLLECTION+":label:"+label, 0, -1)
	if err != nil {
		if err != redis.ErrNil {
			return values, err
		}
	}

	values, err = valuesFromObjects(objects)
	if err != nil {
		return values, err
	}

	return values, nil
}

// Return a list of value descriptors based on their type
func (rc *RedisClient) ValueDescriptorsByType(t string) (values []models.ValueDescriptor, err error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	objects, err := getObjectsByRange(conn, VALUE_DESCRIPTOR_COLLECTION+":type:"+t, 0, -1)
	if err != nil {
		if err != redis.ErrNil {
			return values, err
		}
	}

	values, err = valuesFromObjects(objects)
	if err != nil {
		return values, err
	}

	return values, nil
}

// Delete all value descriptors
func (rc *RedisClient) ScrubAllValueDescriptors() error {
	conn := rc.Pool.Get()
	defer conn.Close()

	err := unlinkCollection(conn, VALUE_DESCRIPTOR_COLLECTION)
	if err != nil {
		return err
	}

	return nil
}

// ************************** HELPER FUNCTIONS ***************************
func addEvent(conn redis.Conn, e *models.Event) (err error) {
	if e.Created == 0 {
		e.Created = time.Now().UnixNano() / int64(time.Millisecond)
	}
	if !e.ID.Valid() {
		e.ID = bson.NewObjectId()
	}
	id := e.ID.Hex()
	m, err := bson.Marshal(e)
	if err != nil {
		return err
	}

	conn.Send("MULTI")
	conn.Send("SET", id, m)
	conn.Send("ZADD", EVENTS_COLLECTION, 0, id)
	conn.Send("ZADD", EVENTS_COLLECTION+":created", e.Created, id)
	conn.Send("ZADD", EVENTS_COLLECTION+":pushed", e.Pushed, id)
	conn.Send("ZADD", EVENTS_COLLECTION+":device:"+e.Device, e.Created, id)

	rids := make([]interface{}, len(e.Readings)*2+1)
	rids[0] = EVENTS_COLLECTION + ":readings:" + id
	for i, r := range e.Readings {
		r.Created = e.Created
		r.Id = bson.NewObjectId()
		err := addReading(conn, false, &r)
		if err != nil {
			return err
		}
		rids[i*2+1] = 0
		rids[i*2+2] = r.Id
	}
	if len(rids) > 1 {
		conn.Send("ZADD", rids...)
	}

	_, err = conn.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}

func deleteEvent(conn redis.Conn, id string) error {
	conn.Send("MULTI")
	conn.Send("UNLINK", id)
	conn.Send("ZRANGE", EVENTS_COLLECTION+":readings:"+id, 0, -1)
	conn.Send("UNLINK", EVENTS_COLLECTION+":readings:"+id)
	conn.Send("ZREM", EVENTS_COLLECTION, id)
	conn.Send("ZREM", EVENTS_COLLECTION+":created", id)
	res, err := redis.Values(conn.Do("EXEC"))
	if err != nil {
		return err
	}
	exists, err := redis.Bool(res[0], nil)
	if !exists {
		return redis.ErrNil
	}

	rids, err := redis.Values(res[1], nil)
	if err != nil {
		return err
	}
	for _, ir := range rids {
		rid, err := redis.String(ir, nil)
		if err != nil {
			return err
		}
		err = deleteReading(conn, rid)
		if err != nil {
			return err
		}
	}

	return nil
}

func eventByID(conn redis.Conn, id string) (event models.Event, err error) {
	obj, err := conn.Do("GET", id)
	if err != nil {
		return event, err
	}

	event, err = eventFromObject(obj)
	if err != nil {
		return event, err
	}

	return event, err
}

// Add a reading to the database
func addReading(conn redis.Conn, tx bool, r *models.Reading) (err error) {
	if r.Created == 0 {
		r.Created = time.Now().UnixNano() / int64(time.Millisecond)
	}
	if !r.Id.Valid() {
		r.Id = bson.NewObjectId()
	}
	id := r.Id.Hex()
	m, err := bson.Marshal(r)
	if err != nil {
		return err
	}

	if tx {
		conn.Send("MULTI")
	}
	conn.Send("SET", id, m)
	conn.Send("ZADD", READINGS_COLLECTION, 0, id)
	conn.Send("ZADD", READINGS_COLLECTION+":created", r.Created, id)
	conn.Send("ZADD", READINGS_COLLECTION+":device:"+r.Device, r.Created, id)
	conn.Send("ZADD", READINGS_COLLECTION+":name:"+r.Name, r.Created, id)
	if tx {
		_, err = conn.Do("EXEC")
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteReading(conn redis.Conn, id string) error {
	r, err := readingByID(conn, id)
	if err != nil {
		return err
	}

	conn.Send("MULTI")
	conn.Send("UNLINK", id)
	conn.Send("ZREM", READINGS_COLLECTION, id)
	conn.Send("ZREM", READINGS_COLLECTION+":created", id)
	conn.Send("ZREM", READINGS_COLLECTION+":device:"+r.Device, id)
	conn.Send("ZREM", READINGS_COLLECTION+":name:"+r.Name, id)
	_, err = conn.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}

func readingByID(conn redis.Conn, id string) (reading models.Reading, err error) {
	obj, err := conn.Do("GET", id)
	if err != nil {
		return reading, err
	}

	err = readingFromObject(obj, &reading)
	if err != nil {
		return reading, err
	}

	return reading, nil
}

func addValue(conn redis.Conn, v *models.ValueDescriptor) (err error) {
	if v.Created == 0 {
		v.Created = time.Now().UnixNano() / int64(time.Millisecond)
	}
	if !v.Id.Valid() {
		v.Id = bson.NewObjectId()
	}
	id := v.Id.Hex()
	m, err := bson.Marshal(v)
	if err != nil {
		return err
	}

	exists, err := redis.Bool(conn.Do("HEXISTS", VALUE_DESCRIPTOR_COLLECTION+":name", v.Name))
	if err != nil {
		return err
	}
	if exists {
		return ErrNotUnique
	}

	conn.Send("MULTI")
	conn.Send("SET", id, m)
	conn.Send("ZADD", VALUE_DESCRIPTOR_COLLECTION, 0, id)
	conn.Send("HSET", VALUE_DESCRIPTOR_COLLECTION+":name", v.Name, id)
	conn.Send("ZADD", VALUE_DESCRIPTOR_COLLECTION+":uomlabel:"+v.UomLabel, 0, id)
	conn.Send("ZADD", VALUE_DESCRIPTOR_COLLECTION+":type:"+v.Type, 0, id)
	for _, label := range v.Labels {
		conn.Send("ZADD", VALUE_DESCRIPTOR_COLLECTION+":label:"+label, 0, id)
	}
	_, err = conn.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}

func deleteValue(conn redis.Conn, id string) error {
	v, err := valueByID(conn, id)
	if err != nil {
		return err
	}

	conn.Send("MULTI")
	conn.Send("UNLINK", id)
	conn.Send("ZREM", VALUE_DESCRIPTOR_COLLECTION, id)
	conn.Send("HDEL", VALUE_DESCRIPTOR_COLLECTION+":name", v.Name)
	conn.Send("ZREM", VALUE_DESCRIPTOR_COLLECTION+":uomlabel:"+v.UomLabel, id)
	conn.Send("ZREM", VALUE_DESCRIPTOR_COLLECTION+":type:"+v.Type, id)
	for _, label := range v.Labels {
		conn.Send("ZREM", VALUE_DESCRIPTOR_COLLECTION+":label:"+label, 0, id)
	}
	_, err = conn.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

func valueByID(conn redis.Conn, id string) (value models.ValueDescriptor, err error) {
	obj, err := conn.Do("GET", id)
	if err != nil {
		return value, err
	}

	value, err = valueFromObject(obj)
	if err == redis.ErrNil {
		return value, ErrNotFound
	}
	if err != nil {
		return value, err
	}

	return value, nil
}

func valueByName(conn redis.Conn, name string) (value models.ValueDescriptor, err error) {
	id, err := redis.String(conn.Do("HGET", VALUE_DESCRIPTOR_COLLECTION+":name", name))
	if err != nil {
		return value, err
	}

	value, err = valueByID(conn, id)
	if err != nil {
		return value, err
	}

	return value, nil
}

// **********************
// Script notes:
// * magic number 4096 is less < 8000 (redis:/deps/lua/lapi.c:LUAI_MAXCSTACK -> unpack error)
// * assume single instance
var scripts = map[string]redis.Script{
	"getObjectsByRange": *redis.NewScript(1, `
		local magic = 4096
		local ids = redis.call('ZRANGE', KEYS[1], ARGV[1], ARGV[2])
		local rep = {}
		if #ids > 0 then
			for i = 1, #ids, magic do
				local temp = redis.call('MGET', unpack(ids, i, i+magic < #ids and i+magic or #ids))
				for _, o in ipairs(temp) do
					table.insert(rep, o)
				end
			end
			return rep
		else
			return nil
		end
		`),
	"getObjectsByRangeFilter": *redis.NewScript(2, `
		local magic = 4096
		local ids = redis.call('ZRANGE', KEYS[1], ARGV[1], ARGV[2])
		local rep = {}
		if #ids > 0 then
			for i, id in ipairs(ids) do
				local v = redis.call('ZSCORE', KEYS[2], id)
				if v == nil then
					ids[i] = nil
				end
			end
			for i = 1, #ids, magic do
				local temp = redis.call('MGET', unpack(ids, i, i+magic < #ids and i+magic or #ids))
				for _, o in ipairs(temp) do
					table.insert(rep, o)
				end
			end
		else
			return nil
		end
		return rep
		`),
	"getObjectsByScore": *redis.NewScript(1, `
		local magic = 4096
		local cmd = {
			'ZRANGEBYSCORE', KEYS[1], ARGV[1],
			tonumber(ARGV[2]) < 0 and '+inf' or ARGV[2],
		}
		if tonumber(ARGV[3]) ~= 0 then
			table.insert(cmd, 'LIMIT')
			table.insert(cmd, 0)
			table.insert(cmd, ARGV[3])
		end
		local ids = redis.call(unpack(cmd))
		local rep = {}
		if #ids > 0 then
			for i = 1, #ids, magic do
				local temp = redis.call('MGET', unpack(ids, i, i+magic < #ids and i+magic or #ids))
				for _, o in ipairs(temp) do
					table.insert(rep, o)
				end
			end
		else
			return nil
		end
		return rep
		`),
	"unlinkZsetMembers": *redis.NewScript(1, `
		local magic = 4096
		local ids = redis.call('ZRANGE', KEYS[1], 0, -1)
		if #ids > 0 then
			for i = 1, #ids, magic do
				redis.call('UNLINK', unpack(ids, i, i+magic < #ids and i+magic or #ids))
			end
		end
		`),
	"unlinkCollection": *redis.NewScript(0, `
		local magic = 4096
		redis.replicate_commands()
		local c = 0
		repeat
			local s = redis.call('SCAN', c, 'MATCH', ARGV[1] .. '*')
			c = tonumber(s[1])
			if #s[2] > 0 then
				redis.call('UNLINK', unpack(s[2]))
			end
		until c == 0
		`),
}

func unlinkCollection(conn redis.Conn, col string) error {
	conn.Send("MULTI")
	s := scripts["unlinkZsetMembers"]
	s.Send(conn, col)
	s = scripts["unlinkCollection"]
	s.Send(conn, col)
	_, err := conn.Do("EXEC")
	if err != nil {
		if err != redis.ErrNil {
			return err
		}
	}

	return err
}

func getObjectsByRangeLua(conn redis.Conn, key string, start, end int) (objects []interface{}, err error) {
	s := scripts["getObjectsByRange"]
	objects, err = redis.Values(s.Do(conn, key, start, end))
	if err != nil {
		return nil, err
	}

	return objects, nil
}

func getObjectsByRangeFilterLua(conn redis.Conn, key string, filter string, start, end int) (objects []interface{}, err error) {
	s := scripts["getObjectsByRangeFilter"]
	objects, err = redis.Values(s.Do(conn, key, filter, start, end))
	if err != nil {
		return nil, err
	}

	return objects, nil
}

// Return objects by a score from a zset
// if limit is 0, all are returned
// if end is negative, it is considered as positive infinity
func getObjectsByScoreLua(conn redis.Conn, key string, start, end int64, limit int) (objects []interface{}, err error) {
	s := scripts["getObjectsByScore"]
	objects, err = redis.Values(s.Do(conn, key, start, end, limit))
	if err != nil {
		return nil, err
	}

	return objects, nil
}

func eventsFromObjects(objects []interface{}) (events []models.Event, err error) {
	for _, o := range objects {
		e, err := eventFromObject(o)
		if err != nil {
			return events, err
		}
		events = append(events, e)
	}
	return events, nil
}

func eventFromObject(o interface{}) (event models.Event, err error) {
	b, err := redis.Bytes(o, nil)
	if err == redis.ErrNil {
		return event, ErrNotFound
	}
	if err != nil {
		return event, err
	}

	err = bson.Unmarshal(b, &event)
	if err != nil {
		return event, err
	}

	return event, nil
}

func readingsFromObjects(objects []interface{}) (readings []models.Reading, err error) {
	readings = make([]models.Reading, len(objects))
	for i, o := range objects {
		r := models.Reading{}
		err := readingFromObject(o, &r)
		if err != nil {
			return readings, err
		}
		readings[i] = r
	}
	return readings, nil
}

func readingFromObject(o interface{}, reading *models.Reading) (err error) {
	b, err := redis.Bytes(o, nil)
	if err != nil {
		return err
	}

	err = bson.Unmarshal(b, reading)
	if err != nil {
		return err
	}

	return nil
}

func valuesFromObjects(objects []interface{}) (values []models.ValueDescriptor, err error) {
	for _, o := range objects {
		v, err := valueFromObject(o)
		if err != nil {
			return values, err
		}
		values = append(values, v)
	}
	return values, nil
}

func valueFromObject(o interface{}) (value models.ValueDescriptor, err error) {
	b, err := redis.Bytes(o, nil)
	if err != nil {
		return value, err
	}

	err = bson.Unmarshal(b, &value)
	if err != nil {
		return value, err
	}

	return value, nil
}

func getObjectsByRangeEmbedded(conn redis.Conn, key string, start, end int) (objects []interface{}, err error) {
	oids, err := redis.Values(conn.Do("ZRANGE", key, start, end))
	if err != nil && err != redis.ErrNil {
		return objects, err
	}

	if len(oids) > 0 {
		objects, err = redis.Values(conn.Do("MGET", oids...))
		if err != nil && err != redis.ErrNil {
			return objects, err
		}
	}
	return objects, nil
}

func getObjectsByRangeFilterEmbedded(conn redis.Conn, key string, filter string, start, end int) (objects []interface{}, err error) {
	oids, err := redis.Strings(conn.Do("ZRANGE", key, start, end))
	if err != nil && err != redis.ErrNil {
		return objects, err
	}
	for _, oid := range oids {
		_, err := conn.Do("ZSCORE", filter, oid)
		if err != nil && err != redis.ErrNil {
			return objects, err
		}
		if err != redis.ErrNil {
			o, err := conn.Do("GET", oid)
			if err != nil && err != redis.ErrNil {
				return objects, err
			}
			objects = append(objects, o)
		}
	}

	return objects, nil
}

func getObjectsByScoreEmbedded(conn redis.Conn, key string, start, end int64, limit int) (objects []interface{}, err error) {
	args := []interface{}{
		key, start,
	}
	if end < 0 {
		args = append(args, "+inf")
	} else {
		args = append(args, end)
	}
	if limit != 0 {
		args = append(args, "LIMIT")
		args = append(args, 0)
		args = append(args, limit)
	}

	oids, err := redis.Strings(conn.Do("ZRANGEBYSCORE", args...))
	if err != nil && err != redis.ErrNil {
		return objects, err
	}

	objects, err = redis.Values(conn.Do("MGET", oids))
	if err != nil && err != redis.ErrNil {
		return objects, err
	}

	return objects, nil
}
