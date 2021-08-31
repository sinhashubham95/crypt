package appconfig

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/appconfig"
	"github.com/bketelsen/crypt/backend"
)

type Client struct {
	client      *appconfig.AppConfig
	application *string
	environment *string
	data        *data
}

type value struct {
	version *string
	value   []byte
}

type data struct {
	mu   sync.RWMutex
	data map[string]*value
}

func New(machines []string) (*Client, error) {
	provider, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewEnvCredentials(),
	})
	if err != nil {
		return nil, fmt.Errorf("creating new aws session for crypt.backend.Client: %v", err)
	}
	appConfig := appconfig.New(provider)
	if len(machines) == 0 {
		return nil, errors.New("application should be defined")
	}
	return &Client{
		client:      appConfig,
		application: aws.String(machines[0]),
		environment: aws.String(os.Getenv("AWS_ENV")),
		data:        &data{data: make(map[string]*value)},
	}, nil
}

func (c *Client) Get(key string) ([]byte, error) {
	// get existing data
	b := c.getExistingData(key)
	if b != nil {
		return b, nil
	}
	// no existing data exists
	config, err := c.getConfiguration(nil, aws.String(key))
	if err != nil {
		return nil, fmt.Errorf("getting configuration for %s: %v", key, err)
	}
	// save and return data
	return c.saveAndReturnData(key, config), nil
}

func (c *Client) List(key string) (backend.KVPairs, error) {
	b, err := c.Get(key)
	if err != nil {
		return nil, err
	}
	return []*backend.KVPair{{
		Key:   key,
		Value: b,
	}}, nil
}

func (c *Client) Set(string, []byte) error {
	return nil
}

func (c *Client) Watch(key string, stop chan bool) <-chan *backend.Response {
	ch := make(chan *backend.Response, 0)
	t := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-t.C:
				config, err := c.getConfiguration(c.getExistingVersion(key), aws.String(key))
				if err == nil {
					ch <- &backend.Response{
						Value: c.saveAndReturnData(key, config),
						Error: nil,
					}
				}
				if err != nil {
					time.Sleep(time.Second * 5)
				}
			case <-stop:
				close(ch)
				return
			}
		}
	}()
	return ch
}

func (c *Client) getExistingData(key string) []byte {
	c.data.mu.RLock()
	defer c.data.mu.RUnlock()
	if v, ok := c.data.data[key]; ok {
		b := make([]byte, len(v.value))
		copy(b, v.value)
		return b
	}
	return nil
}

func (c *Client) getExistingVersion(key string) *string {
	c.data.mu.RLock()
	defer c.data.mu.RUnlock()
	if v, ok := c.data.data[key]; ok {
		return v.version
	}
	return nil
}

func (c *Client) getConfiguration(version, configuration *string) (*appconfig.GetConfigurationOutput, error) {
	return c.client.GetConfiguration(&appconfig.GetConfigurationInput{
		Application:                c.application,
		ClientId:                   aws.String("crypt-app-config"),
		Configuration:              configuration,
		ClientConfigurationVersion: version,
		Environment:                c.environment,
	})
}

func (c *Client) saveAndReturnData(key string, config *appconfig.GetConfigurationOutput) []byte {
	c.data.mu.Lock()
	defer c.data.mu.Unlock()
	c.data.data[key] = &value{
		version: config.ConfigurationVersion,
		value:   config.Content,
	}
	b := make([]byte, len(config.Content))
	copy(b, config.Content)
	return b
}
