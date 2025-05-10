package config

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"main.go/app"
	"strings"
	"time"
)

type KafkaConfig struct {
	BootstrapServers    []string            `mapstructure:"bootstrapServers"`
	Topic               string              `mapstructure:"topic"`
	NumPartitions       int32               `mapstructure:"numPartitions"`
	ReplicationFactor   int16               `mapstructure:"replicationFactor"`
	Retention           string              `mapstructure:"retention"`
	ConsumerGroup       string              `mapstructure:"consumerGroup"`
	KafkaProducerConfig KafkaProducerConfig `mapstructure:"producer"`
	KafkaConsumerConfig KafkaConsumerConfig `mapstructure:"consumer"`
}

type KafkaProducerConfig struct {
	RetryMax      int           `mapstructure:"retryMax"`
	ReturnSuccess bool          `mapstructure:"returnSuccess"`
	RetryBackoff  time.Duration `mapstructure:"retryBackoff"`
	ClientId      string        `mapstructure:"clientId"`
}

type KafkaConsumerConfig struct {
	ReturnErrors        bool          `mapstructure:"returnErrors"`
	ClientId            string        `mapstructure:"clientId"`
	NackResendSleep     time.Duration `mapstructure:"nackResendSleep"`
	ReconnectRetrySleep time.Duration `mapstructure:"reconnectRetrySleep"`
}

type OtlpConfig struct {
	Endpoint string `mapstructure:"endpoint"`
}

type HttpServerConfig struct {
	Address        string        `mapstructure:"address"`
	ReadTimeout    time.Duration `mapstructure:"readTimeout"`
	WriteTimeout   time.Duration `mapstructure:"writeTimeout"`
	MaxHeaderBytes int           `mapstructure:"maxHeaderBytes"`
}

type MigrationConfig struct {
	MigrationTable    string        `mapstructure:"migrationTable"`
	StatementDuration time.Duration `mapstructure:"statementDuration"`
}

type PostgreSQLConfig struct {
	Url                string          `mapstructure:"url"`
	MaxOpenConnections int             `mapstructure:"maxOpenConnections"`
	MaxIdleConnections int             `mapstructure:"maxIdleConnections"`
	MaxLifetime        time.Duration   `mapstructure:"maxLifetime"`
	MigrationConfig    MigrationConfig `mapstructure:"migration"`
}

type CqrsConfig struct {
	SleepBeforeEvent                time.Duration `mapstructure:"sleepBeforeEvent"`
	CheckAreEventsProcessedInterval time.Duration `mapstructure:"checkAreEventsProcessedInterval"`
}

type AppConfig struct {
	KafkaConfig      KafkaConfig      `mapstructure:"kafka"`
	OtlpConfig       OtlpConfig       `mapstructure:"otlp"`
	PostgreSQLConfig PostgreSQLConfig `mapstructure:"postgresql"`
	HttpServerConfig HttpServerConfig `mapstructure:"server"`
	CqrsConfig       CqrsConfig       `mapstructure:"cqrs"`
}

//go:embed config-dev
var configFs embed.FS

func CreateTypedConfig() (*AppConfig, error) {
	conf := AppConfig{}
	viper.SetConfigType("yaml")

	if embedBytes, err := configFs.ReadFile("config-dev/config.yml"); err != nil {
		panic(fmt.Errorf("Fatal error during reading embedded config file: %s \n", err))
	} else if err := viper.ReadConfig(bytes.NewBuffer(embedBytes)); err != nil {
		panic(fmt.Errorf("Fatal error during viper reading embedded config file: %s \n", err))
	}

	viper.SetEnvPrefix(strings.ToUpper(app.TRACE_RESOURCE))
	viper.AutomaticEnv()
	err := viper.GetViper().Unmarshal(&conf)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("config file loaded failed. %v\n", err))
	}

	return &conf, nil
}
