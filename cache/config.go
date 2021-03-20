package cache

type (
	ClusterConf []NodeConf

	NodeConf struct {
		Host   string
		Type   string `json:",default=node,options=node|cluster"`
		Pass   string `json:",optional"`
		Weight int    `json:",default=100"`
	}
	CacheConf = ClusterConf
)

func (rc NodeConf) NewRedis() *Redis {
	return NewRedis(rc.Host, rc.Type, rc.Pass)
}
