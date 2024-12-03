package shardctrler

// 定义一个接口，该接口有如下方法
type ConfigStateMachine interface {
	Join(groups map[int][]string) Err
	Leave(gids []int) Err
	Move(shard, gid int) Err
	Query(num int) (Config, Err)
}

type MemoryConfigStateMachine struct {
	Configs []Config
}

func NewMemoryConfigStateMachine() *MemoryConfigStateMachine {
	cf := &MemoryConfigStateMachine{make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers) // 按下标进行值拷贝 i.e.深拷贝
		newGroups[gid] = newServers
	}
	return newGroups
}

// return the gid-group contain which shards
func Group2Shards(config Config) map[int][]int {
	group2shards := make(map[int][]int)
	for gid := range config.Groups {
		group2shards[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		group2shards[gid] = append(group2shards[gid], shard)
	}
	return group2shards
}

// return the group's id with the maximum shards
func GetGIDWithMaxAndMinShards(group2shards map[int][]int) (maxGID, minGID int) {
	cntMax, cntMin := 0, 1000
	maxGID, minGID = -1, -1
	f := true

	// GID-0是设置的初始group，是不存在的，所以GID-0如果有shard就要转出
	if shards, ok := group2shards[0]; ok && len(shards) != 0 {
		maxGID, f = 0, false
	}

	// todo：这里为什么要先排个序，按照升序找最大最小？
	// A: 这里不排序直接找最大最小也行，找最大最小跟顺序没关系
	//gids := []int{}
	//for gid := range group2shards {
	//	gids = append(gids, gid)
	//}
	//sort.Ints(gids)
	//
	//for _, gid := range gids {
	//	if f && len(group2shards[gid]) > cntMax {
	//		maxGID, cntMax = gid, len(group2shards[gid])
	//	}
	//	if gid != 0 && len(group2shards[gid]) < cntMin {
	//		// can't move shard to group_0
	//		minGID, cntMin = gid, len(group2shards[gid])
	//	}
	//}
	for gid, Shards := range group2shards {
		if f && len(Shards) > cntMax {
			maxGID, cntMax = gid, len(Shards)
		}
		if gid != 0 && len(Shards) < cntMin {
			// can't move shard to group_0
			minGID, cntMin = gid, len(Shards)
		}
	}
	return maxGID, minGID
}

// Join adds new groups to configuration
func (cf *MemoryConfigStateMachine) Join(groups map[int][]string) Err {
	lastConfig := cf.Configs[len(cf.Configs)-1] // 最新的Configuration
	// create a new configuration
	newConfig := Config{
		Num:    len(cf.Configs),
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups), // map is reference, so here is a deepCopy
	}

	// add the server which in groups if the server doesn't exist in the last configuration
	for gid, servers := range groups {
		if _, exist := newConfig.Groups[gid]; !exist {
			// gid 的 group 不存在 lastConfiguration 中
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	// find the each groups has which shard
	group2shards := Group2Shards(newConfig)

	// load balance the shards among the groups
	// by maximum groups giving a shard to minimum groups
	// until (maximum groups - minimum groups) <= 1
	for {
		source, target := GetGIDWithMaxAndMinShards(group2shards)
		if source != 0 && len(group2shards[source])-len(group2shards[target]) <= 1 {
			break
		}
		group2shards[target] = append(group2shards[target], group2shards[source][0])
		group2shards[source] = group2shards[source][1:]
	}
	// update newConfig.Shards
	var newShards [NShards]int // 数组
	for gid, shards := range group2shards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards

	// append the newConfig to cf.Configs
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

// Leave allows some groups named gids to leave
func (cf *MemoryConfigStateMachine) Leave(gids []int) Err {
	// get the lastConfig
	lastConfig := cf.Configs[len(cf.Configs)-1]

	// create the newConfig
	newConfig := Config{
		Num:    len(cf.Configs),
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups),
	}

	// collect the orphan shards owned by Leaving group
	group2Shards := Group2Shards(newConfig)
	orphanShards := []int{}
	for _, gid := range gids {
		// delete group_gid that exists in newConfig
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}

		// delete group_gid that exists in group2Shards
		if shards, ok := group2Shards[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(group2Shards, gid)
		}
	}

	// update the newConfig.Shards
	var newShards [NShards]int
	if len(newConfig.Groups) > 0 {
		// re-assign the orphan shards to remain groups
		for _, shard := range orphanShards {
			_, MinGID := GetGIDWithMaxAndMinShards(group2Shards)
			// todo: different with blog
			group2Shards[MinGID] = append(group2Shards[MinGID], shard)
		}

		for gid, shards := range group2Shards {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

// Move allows shard to be assigned to group named gid
func (cf *MemoryConfigStateMachine) Move(shard, gid int) Err {
	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{
		Num:    len(cf.Configs),
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups),
	}

	newConfig.Shards[shard] = gid
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

// Query return a configuration. if num < 0 Or num >= len(Configs) return lastConfig
func (cf *MemoryConfigStateMachine) Query(num int) (config Config, e Err) {
	if num < 0 || num >= len(cf.Configs) {
		return cf.Configs[len(cf.Configs)-1], OK
	}
	return cf.Configs[num], OK
}
