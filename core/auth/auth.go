package auth

import (
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

type User struct {
	Name      string   `yaml:"name"`
	Password  string   `yaml:"password"` // plaintext in dev yaml only
	Bcrypt    string   `yaml:"password_bcrypt"`
	Publish   []string `yaml:"publish"`
	Subscribe []string `yaml:"subscribe"`
	Admin     bool     `yaml:"admin"`
}

type Config struct {
	Users []User `yaml:"users"`
}

type Store struct {
	users map[string]*User
}

func Load(path string) (*Store, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	s := &Store{users: make(map[string]*User)}
	for i := range cfg.Users {
		u := cfg.Users[i]
		s.users[u.Name] = &u
	}
	return s, nil
}

func (s *Store) Authenticate(username, password string) (*User, bool) {
	if s == nil {
		return nil, true // auth disabled
	}
	u, ok := s.users[username]
	if !ok {
		return nil, false
	}
	if u.Bcrypt != "" {
		if err := bcrypt.CompareHashAndPassword([]byte(u.Bcrypt), []byte(password)); err != nil {
			return nil, false
		}
		return u, true
	}
	if u.Password != "" && u.Password == password {
		return u, true
	}
	return nil, false
}

func matchTopic(pattern, topic string) bool {
	if pattern == "#" {
		return true
	}
	pParts := strings.Split(pattern, "/")
	tParts := strings.Split(topic, "/")
	for i, p := range pParts {
		if p == "#" {
			return true
		}
		if i >= len(tParts) {
			return false
		}
		if p != "+" && p != tParts[i] {
			return false
		}
	}
	return len(tParts) == len(pParts)
}

func (u *User) CanPublish(topic string) bool {
	if u.Admin {
		return true
	}
	for _, p := range u.Publish {
		if matchTopic(p, topic) {
			return true
		}
	}
	return len(u.Publish) == 0
}

func (u *User) CanSubscribe(filter string) bool {
	if u.Admin {
		return true
	}
	for _, p := range u.Subscribe {
		if matchTopic(p, filter) {
			return true
		}
	}
	return len(u.Subscribe) == 0
}

// WriteDefaultDev writes a sample users file if missing.
func WriteDefaultDev(dir string) error {
	path := filepath.Join(dir, "users.yaml")
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	_ = os.MkdirAll(dir, 0o755)
	sample := `users:
  - name: admin
    password: admin
    admin: true
    publish: ["#"]
    subscribe: ["#"]
  - name: pub
    password: pub
    publish: ["demo/#"]
    subscribe: []
  - name: sub
    password: sub
    publish: []
    subscribe: ["demo/#"]
`
	return os.WriteFile(path, []byte(sample), 0o644)
}
