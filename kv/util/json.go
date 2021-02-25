package util

import (
	"encoding/json"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	"time"
)

func JsonExpr(data interface{}) string {
	if data == nil {
		return ""
	}
	expr, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return fmt.Sprintf("%+v", data)
	}
	return string(expr)
}


// 给raft加一个messageHandler, 根据角色的不同, 装配不同的handler, 可以节省一堆if逻辑, 不用在每个函数都if一下, 直接装配一个对应角色的handler来干掉if, 写的时候也只需要关注对应角色就可以了
// 其实不只是message, 角色转换和装配也可以放, 就弄一个struct, 弱引用raft就行了. raft强引用这个结构体
func Reset(timer time.Timer, duration time.Duration) {
	timer.Reset(duration)
	select {
	case <-timer.C:
		log.Info("timer reset with channel not empty")
	default:
		break
	}
}