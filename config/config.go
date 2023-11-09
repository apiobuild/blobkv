package config

// Storage ...
type Storage struct {
	Creds  string `yaml:"creds"`
	Bucket string `yaml:"bucket"`
	Prefix string `yaml:"prefix"`
}
