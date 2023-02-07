package config

import (
	"github.com/spf13/viper"
	"strings"
)

type AppConfig struct {
	App   App   `mapstructure:"app"`
	Kafka Kafka `mapstructure:"kafka"`
}

type App struct {
	Name     string `mapstructure:"name"`
	LogLevel string `mapstructure:"log-level"`
}

type Kafka struct {
	BootstrapServers string `mapstructure:"bootstrap-servers"`
	InputTopic       string `mapstructure:"input-topic"`
	OutputTopic      string `mapstructure:"output-topic"`
	SecurityProtocol string `mapstructure:"security-protocol"`
	Ssl              Ssl    `mapstructure:"ssl"`
}

type Ssl struct {
	CaLocation          string `mapstructure:"ca-location"`
	CertificateLocation string `mapstructure:"certificate-location"`
	KeyLocation         string `mapstructure:"key-location"`
	KeyPassword         string `mapstructure:"key-password"`
}

func LoadConfig(path string) (config AppConfig, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("yml")

	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(`.`, `_`, `-`, `_`))

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}
