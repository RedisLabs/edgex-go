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

	"github.com/edgexfoundry/edgex-go/core/domain/models"
	"github.com/garyburd/redigo/redis"
	"gopkg.in/mgo.v2/bson"
)

var currentRedisClient *RedisClient // Singleton used so that RedisEvent can use it to de-reference readings

// TODO: use a single connection
// RedisClient represent a client
type RedisClient struct {
	Pool redis.Pool // Connections to Redis
}

// Return a pointer to the RedisClient
func newRedisClient(config DBConfiguration) (*RedisClient, error) {
	connectionString := config.Host + ":" + strconv.Itoa(config.Port)
	loggingClient.Info("INFO: Connecting to Redis at: " + connectionString)
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			// TODO: add timeout and password from config
			conn, err := redis.Dial("tcp", connectionString)
			if err != nil {
				loggingClient.Error("Error dialing the Redis server: " + err.Error())
				return nil, err
			}
			return conn, nil
		},
	}

	rc := &RedisClient{Pool: *pool}
	currentRedisClient = rc // Set the singleton
	return rc, nil
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
func (rc *RedisClient) Events() ([]models.Event, error) {
	return nil, nil
}

// Add a new event
// UnexpectedError - failed to add to database
// NoValueDescriptor - no existing value descriptor for a reading in the event
func (rc *RedisClient) AddEvent(e *models.Event) (bson.ObjectId, error) {
	e.Created = time.Now().UnixNano() / int64(time.Millisecond)
	e.ID = bson.NewObjectId()
	return e.ID, nil
}

// Update an event - do NOT update readings
// UnexpectedError - problem updating in database
// NotFound - no event with the ID was found
func (rc *RedisClient) UpdateEvent(e models.Event) error {
	return nil
}

// Get an event by id
func (rc *RedisClient) EventById(id string) (models.Event, error) {
	return models.Event{}, nil
}

// Get the number of events in Core Data
func (rc *RedisClient) EventCount() (int, error) {
	return 0, nil
}

// Get the number of events in Core Data for the device specified by id
func (rc *RedisClient) EventCountByDeviceId(id string) (int, error) {
	return 0, nil
}

// Update an event by ID
// Set the pushed variable to the current time
// 404 - Event not found
// 503 - Unexpected problems
//UpdateEventById(id string) error

// Delete an event by ID and all of its readings
// 404 - Event not found
// 503 - Unexpected problems
func (rc *RedisClient) DeleteEventById(id string) error {
	return nil
}

// Get a list of events based on the device id and limit
func (rc *RedisClient) EventsForDeviceLimit(id string, limit int) ([]models.Event, error) {
	return nil, nil
}

// Get a list of events based on the device id
func (rc *RedisClient) EventsForDevice(id string) ([]models.Event, error) {
	return nil, nil
}

// Delete all of the events by the device id (and the readings)
//DeleteEventsByDeviceId(id string) error

// Return a list of events whos creation time is between startTime and endTime
// Limit the number of results by limit
func (rc *RedisClient) EventsByCreationTime(startTime, endTime int64, limit int) ([]models.Event, error) {
	return nil, nil
}

// Return a list of readings for a device filtered by the value descriptor and limited by the limit
// The readings are linked to the device through an event
func (rc *RedisClient) ReadingsByDeviceAndValueDescriptor(deviceId, valueDescriptor string, limit int) ([]models.Reading, error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	readings := []models.Reading{}
	if limit == 0 {
		return readings, nil
	}

	// TODO: this needs to be optimized for value search
	elems, err := redis.Strings(conn.Do("ZRANGE", READINGS_COLLECTION+":device:"+deviceId, 0, -1))
	if err != nil {
		return readings, err
	}

	// TODO: pipeline this
	for _, elem := range elems {
		hash, err := redis.Strings(conn.Do("HGETALL", elem))
		if err != nil {
			return readings, err
		}

		reading, err := rc.hashToReading(hash)
		if err != nil {
			return readings, err
		}

		// Filter by valueDescriptor/Name
		if reading.Name == valueDescriptor {
			readings = append(readings, reading)
		}
		if len(readings) == limit {
			break
		}
	}
	return readings, nil
}

// Remove all the events that are older than the given age
// Return the number of events removed
//RemoveEventByAge(age int64) (int, error)

// Get events that are older than a age
func (rc *RedisClient) EventsOlderThanAge(age int64) ([]models.Event, error) {
	return nil, nil
}

// Remove all the events that have been pushed
//func (dbc *DBClient) ScrubEvents()(int, error)

// Get events that have been pushed (pushed field is not 0)
func (rc *RedisClient) EventsPushed() ([]models.Event, error) {
	return nil, nil
}

// Delete all readings and events
func (rc *RedisClient) ScrubAllEvents() error {
	conn := rc.Pool.Get()
	defer conn.Close()

	_, err := conn.Do("FLUSHDB")
	if err != nil {
		return err
	}
	return nil
}

// ********************* READING FUNCTIONS *************************
// Return a list of readings sorted by reading id
func (rc *RedisClient) Readings() ([]models.Reading, error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	readings := []models.Reading{}
	elems, err := redis.Strings(conn.Do("ZRANGE", READINGS_COLLECTION, 0, -1))
	if err != nil {
		return readings, err
	}

	// TODO: pipeline this
	for _, elem := range elems {
		hash, err := redis.Strings(conn.Do("HGETALL", elem))
		if err != nil {
			return readings, err
		}

		reading, err := rc.hashToReading(hash)
		if err != nil {
			return readings, err
		}

		readings = append(readings, reading)
	}

	return readings, nil
}

// Post a new reading
// Check if valuedescriptor exists in the database
func (rc *RedisClient) AddReading(r models.Reading) (bson.ObjectId, error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	r.Created = time.Now().UnixNano() / int64(time.Millisecond)
	r.Id = bson.NewObjectId()

	err := rc.addReadingToDB(r)
	if err != nil {
		return r.Id, err
	}

	return r.Id, nil
}

// Update a reading
// 404 - reading cannot be found
// 409 - Value descriptor doesn't exist
// 503 - unknown issues
func (rc *RedisClient) UpdateReading(r models.Reading) error {
	conn := rc.Pool.Get()
	defer conn.Close()

	o, err := rc.ReadingById(r.Id.Hex())
	if err != nil {
		return err
	}

	r.Created = o.Created
	r.Modified = time.Now().UnixNano() / int64(time.Millisecond)

	err = rc.DeleteReadingById(r.Id.Hex())
	if err != nil {
		return err
	}

	err = rc.addReadingToDB(r)
	if err != nil {
		return err
	}

	return nil
}

// Get a reading by ID
func (rc *RedisClient) ReadingById(id string) (models.Reading, error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	hash, err := redis.Strings(conn.Do("HGETALL", id))
	if err != nil {
		return models.Reading{}, err
	}
	if len(hash) == 0 {
		return models.Reading{}, ErrNotFound
	}

	reading, err := rc.hashToReading(hash)
	if err != nil {
		return models.Reading{}, err
	}

	return reading, nil
}

// Get the number of readings in core data
func (rc *RedisClient) ReadingCount() (int, error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	count, err := redis.Int(conn.Do("ZCARD", READINGS_COLLECTION))
	if err != nil {
		return -1, err
	}

	return count, nil
}

// Delete a reading by ID
// 404 - can't find the reading with the given id
func (rc *RedisClient) DeleteReadingById(id string) error {
	conn := rc.Pool.Get()
	defer conn.Close()

	r, err := rc.ReadingById(id)
	if err != nil {
		return err
	}

	conn.Send("MULTI")
	conn.Send("DEL", id)
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

// Return a list of readings for the given device (id or name)
// 404 - meta data checking enabled and can't find the device
// Sort the list of readings on creation date
func (rc *RedisClient) ReadingsByDevice(id string, limit int) ([]models.Reading, error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	readings := []models.Reading{}
	if limit == 0 {
		return readings, nil
	}

	// TODO: this needs to be optimized for value search
	elems, err := redis.Strings(conn.Do("ZRANGE", READINGS_COLLECTION+":device:"+id, 0, -1))
	if err != nil {
		return readings, err
	}

	// TODO: pipeline this
	for _, elem := range elems {
		hash, err := redis.Strings(conn.Do("HGETALL", elem))
		if err != nil {
			return readings, err
		}

		reading, err := rc.hashToReading(hash)
		if err != nil {
			return readings, err
		}

		readings = append(readings, reading)
		if len(readings) == limit {
			break
		}
	}
	return readings, nil
}

// Return a list of readings for the given value descriptor
// 413 - the number exceeds the current max limit
func (rc *RedisClient) ReadingsByValueDescriptor(name string, limit int) ([]models.Reading, error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	readings := []models.Reading{}
	if limit == 0 {
		return readings, nil
	}

	elems, err := redis.Strings(conn.Do("ZRANGE", READINGS_COLLECTION+":name:"+name, 0, limit-1))
	if err != nil {
		return readings, err
	}

	// TODO: pipeline this
	for _, elem := range elems {
		hash, err := redis.Strings(conn.Do("HGETALL", elem))
		if err != nil {
			return readings, err
		}

		reading, err := rc.hashToReading(hash)
		if err != nil {
			return readings, err
		}

		readings = append(readings, reading)
	}
	return readings, nil
}

// Return a list of readings whose name is in the list of value descriptor names
func (rc *RedisClient) ReadingsByValueDescriptorNames(names []string, limit int) ([]models.Reading, error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	readings := []models.Reading{}
	if limit == 0 {
		return readings, nil
	}

	for _, name := range names {
		elems, err := redis.Strings(conn.Do("ZRANGE", READINGS_COLLECTION+":name:"+name, 0, limit-1))
		if err != nil {
			return readings, err
		}

		// TODO: pipeline this
		for _, elem := range elems {
			hash, err := redis.Strings(conn.Do("HGETALL", elem))
			if err != nil {
				return readings, err
			}

			reading, err := rc.hashToReading(hash)
			if err != nil {
				return readings, err
			}

			readings = append(readings, reading)
			if len(readings) == limit {
				break
			}
		}
		if len(readings) == limit {
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
func (rc *RedisClient) ReadingsByCreationTime(start, end int64, limit int) ([]models.Reading, error) {
	conn := rc.Pool.Get()
	defer conn.Close()

	readings := []models.Reading{}
	elems, err := redis.Strings(conn.Do("ZRANGEBYSCORE", READINGS_COLLECTION+":created", start, end))
	if err != nil {
		return readings, err
	}

	// TODO: pipeline this
	for _, elem := range elems {
		hash, err := redis.Strings(conn.Do("HGETALL", elem))
		if err != nil {
			return readings, err
		}

		reading, err := rc.hashToReading(hash)
		if err != nil {
			return readings, err
		}

		readings = append(readings, reading)
		if len(readings) == limit {
			break
		}
	}

	return readings, nil
}

func (rc *RedisClient) readingToHash(reading models.Reading) ([]interface{}, error) {
	hash := []interface{}{
		"id", reading.Id.Hex(),
		"pushed", reading.Pushed,
		"created", reading.Created,
		"origin", reading.Origin,
		"modified", reading.Modified,
		"device", reading.Device,
		"name", reading.Name,
		"value", reading.Value,
		"_type", "reading",
	}
	return hash, nil
}

// Converts to a reading from hash format
func (rc *RedisClient) hashToReading(hash []string) (models.Reading, error) {
	var reading models.Reading
	i := 0
	for i < len(hash) {
		v := hash[i+1]
		switch hash[i] {
		case "id":
			reading.Id = bson.ObjectIdHex(v)
		case "pushed":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return reading, err
			}
			reading.Pushed = n
		case "created":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return reading, err
			}
			reading.Created = n
		case "origin":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return reading, err
			}
			reading.Origin = n
		case "modified":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return reading, err
			}
			reading.Modified = n
		case "device":
			reading.Device = v
		case "name":
			reading.Name = v
		case "value":
			reading.Value = v
		}
		i = i + 2
	}
	return reading, nil
}

func (rc *RedisClient) addReadingToDB(r models.Reading) error {
	conn := rc.Pool.Get()
	defer conn.Close()

	hash, err := rc.readingToHash(r)
	if err != nil {
		return err
	}

	keyhash := append([]interface{}{r.Id.Hex()}, hash...)

	conn.Send("MULTI")
	conn.Send("HSET", keyhash...)
	conn.Send("ZADD", READINGS_COLLECTION, 0, r.Id.Hex())
	conn.Send("ZADD", READINGS_COLLECTION+":created", r.Created, r.Id.Hex())
	conn.Send("ZADD", READINGS_COLLECTION+":device:"+r.Device, r.Created, r.Id.Hex())
	conn.Send("ZADD", READINGS_COLLECTION+":name:"+r.Name, r.Created, r.Id.Hex())
	_, err = conn.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}

// ************************** VALUE DESCRIPTOR FUNCTIONS ***************************
// Add a value descriptor
// 409 - Formatting is bad or it is not unique
// 503 - Unexpected
// TODO: Check for valid printf formatting
func (rc *RedisClient) AddValueDescriptor(v models.ValueDescriptor) (bson.ObjectId, error) {
	v.Created = time.Now().UnixNano() / int64(time.Millisecond)
	v.Id = bson.NewObjectId()
	return v.Id, nil
}

// Return a list of all the value descriptors
// 513 Service Unavailable - database problems
func (rc *RedisClient) ValueDescriptors() ([]models.ValueDescriptor, error) {
	return nil, nil
}

// Update a value descriptor
// First use the ID for identification, then the name
// TODO: Check for the valid printf formatting
// 404 not found if the value descriptor cannot be found by the identifiers
func (rc *RedisClient) UpdateValueDescriptor(v models.ValueDescriptor) error {
	return nil
}

// Delete a value descriptor based on the ID
func (rc *RedisClient) DeleteValueDescriptorById(id string) error {
	return nil
}

// Return a value descriptor based on the name
func (rc *RedisClient) ValueDescriptorByName(name string) (models.ValueDescriptor, error) {
	return models.ValueDescriptor{}, nil
}

// Return value descriptors based on the names
func (rc *RedisClient) ValueDescriptorsByName(names []string) ([]models.ValueDescriptor, error) {
	return nil, nil
}

// Delete a valuedescriptor based on the name
//DeleteValueDescriptorByName(name string) error

// Return a value descriptor based on the id
func (rc *RedisClient) ValueDescriptorById(id string) (models.ValueDescriptor, error) {
	return models.ValueDescriptor{}, nil
}

// Return value descriptors based on the unit of measure label
func (rc *RedisClient) ValueDescriptorsByUomLabel(uomLabel string) ([]models.ValueDescriptor, error) {
	return nil, nil
}

// Return value descriptors based on the label
func (rc *RedisClient) ValueDescriptorsByLabel(label string) ([]models.ValueDescriptor, error) {
	return nil, nil
}

// Return a list of value descriptors based on their type
func (rc *RedisClient) ValueDescriptorsByType(t string) ([]models.ValueDescriptor, error) {
	return nil, nil
}

// Delete all value descriptors
func (rc *RedisClient) ScrubAllValueDescriptors() error {
	return nil
}
