package scache

import (
	"encoding/json"
	"fmt"
	"github.com/didi/nightingale/src/modules/monapi/config"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/didi/nightingale/src/models"

	"github.com/toolkits/pkg/logger"
)

type OCStraResult struct {
	ErrCode int    `json:"code"`
	ErrMsg  string `json:"msg"`
	Data	[]*models.Stra `json:"data"`
}

func SyncStras() {
	t1 := time.NewTicker(time.Duration(CHECK_INTERVAL) * time.Second)

	syncStras()
	logger.Info("[cron] sync stras start...")
	for {
		<-t1.C
		syncStras()
	}
}

func syncStras() {
	stras, err := models.EffectiveStrasList()
	if err != nil {
		logger.Error("sync stras err:", err)
		return
	}
	strasMap := make(map[string][]*models.Stra)
	for _, stra := range stras {
		if stra.Category == 2 {
			needChildNids := true
			for _, e := range stra.Exprs {
				if e.Metric == "nodata" {
					needChildNids = false
					break
				}
			}

			//只有非nodata的告警策略，才支持告警策略继承，否则nodata会有误报
			if needChildNids {
				nids, err := models.GetRelatedNidsForMon(stra.Nid, stra.ExclNid)
				if err != nil {
					logger.Warningf("get LeafNids err:%v %v", err, stra)
					continue
				}

				for _, nid := range nids {
					stra.Nids = append(stra.Nids, strconv.FormatInt(nid, 10))
				}
			}

			stra.Nids = append(stra.Nids, strconv.FormatInt(stra.Nid, 10))
		} else {
			//增加叶子节点nid
			stra.LeafNids, err = models.GetLeafNidsForMon(stra.Nid, stra.ExclNid)
			if err != nil {
				logger.Warningf("get LeafNids err:%v %v", err, stra)
				continue
			}

			var hosts []string
			for _, nid := range stra.LeafNids {
				hs, err := HostUnderNode(nid)
				if err != nil {
					logger.Warningf("get hosts err:%v %v", err, stra)
					continue
				}
				hosts = append(hosts, hs...)
			}

			hostFilter := make(map[string]struct{})
			for _, host := range hosts {
				if _, exists := hostFilter[host]; exists {
					continue
				}
				hostFilter[host] = struct{}{}
				stra.Endpoints = append(stra.Endpoints, host)
			}
		}

		node, err := JudgeHashRing.GetNode(strconv.FormatInt(stra.Id, 10))
		if err != nil {
			logger.Warningf("get node err:%v %v", err, stra)
			continue
		}
		if _, exists := strasMap[node]; exists {
			strasMap[node] = append(strasMap[node], stra)
		} else {
			strasMap[node] = []*models.Stra{stra}
		}
	}

	//拉取opencloud策略
	cfg := config.Get()
	if cfg.Opencloud.Enable {
		url := cfg.Opencloud.Stra
		ocstras, err := jsonOCStraGet(url)
		if err != nil {
			logger.Error(fmt.Errorf("get opencloud stras failed: %s", err))
			fmt.Println(fmt.Errorf("get opencloud stras failed: %s", err))
		} else {
			logger.Info("get opencloud stras successfully")
			fmt.Println("get opencloud stras successfully")
			for _, stra := range ocstras {
				node, err := JudgeHashRing.GetNode("oc" + strconv.FormatInt(stra.Id, 10))
				if err != nil {
					logger.Warningf("get oc node err:%v %v", err, stra)
					fmt.Printf("get oc node err:%v %v", err, stra)
					continue
				}
				// 补充时区
				loc := time.Local
				stra.Created = stra.Created.In(loc)
				stra.LastUpdated = stra.LastUpdated.In(loc)

				if _, exists := strasMap[node]; exists {
					strasMap[node] = append(strasMap[node], stra)
				} else {
					strasMap[node] = []*models.Stra{stra}
				}
			}
		}
	}

	StraCache.SetAll(strasMap)
}

func CleanStraLoop() {
	duration := time.Second * time.Duration(300)
	for {
		time.Sleep(duration)
		cleanStra()
	}
}

//定期清理没有找到nid的策略
func cleanStra() {
	list, err := models.StrasAll()
	if err != nil {
		logger.Errorf("get stras fail: %v", err)
		return
	}

	for _, stra := range list {
		node, err := models.NodeGet("id=?", stra.Nid)
		if err != nil {
			logger.Warningf("get node failed, node id: %d, err: %v", stra.Nid, err)
			continue
		}

		if node == nil {
			logger.Infof("delete stra:%d", stra.Id)
			if err := models.StraDel(stra.Id); err != nil {
				logger.Warningf("delete stra: %d, err: %v", stra.Id, err)
			}
		}
	}
}


func jsonOCStraGet(url string) ([]*models.Stra, error) {
	r, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	if r.Body == nil {
		return nil, fmt.Errorf("response body of %s is nil", url)
	}

	defer r.Body.Close()

	resp, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read req body err: %v", err)
	}

	result := OCStraResult{}

	err = json.Unmarshal(resp, &result)
	if err != nil {
		return nil, fmt.Errorf("unmarshal resp content err: %v", err)
	}

	if result.ErrCode != 0 {
		err = fmt.Errorf("req return ErrCode = %d ErrMsg = %s", result.ErrCode, result.ErrMsg)
		return nil, err
	}

	return result.Data, nil
}
