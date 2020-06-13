package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

//profile variables
type Conf struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	ApplicationName string `yaml:"applicationName"`
	Version         string `yaml:"version"`
	Group           string `yaml:"group"`
	RegistryAddress string
}

func GetConf(fileName string, conf *Conf) error {
	yamlFile, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(yamlFile, conf)
	if err != nil {
		return err
	}
	return nil
}
