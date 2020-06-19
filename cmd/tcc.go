package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"github.com/blastbao/tcc/constant"
	"github.com/blastbao/tcc/global/various"
	"github.com/blastbao/tcc/log"
	"github.com/blastbao/tcc/model"
	"github.com/blastbao/tcc/store/data"
	"github.com/blastbao/tcc/util"
	"time"
)

type tcc interface {

	// r：原生Request请求
	// api：根据当前请求，从配置文件中获取的 Try 的 URL 信息
	// 返回值：
	// 	1、尝试过程中，成功的步骤
	// 	2、错误信息
	Try(r *http.Request, api *model.RuntimeApi) ([]*data.SuccessStep, error)

	// r：原生Request请求
	// api：根据当前请求，从配置文件中获取的 Confirm 的 URL 信息
	// 返回值：
	// 	1、错误信息
	Confirm(r *http.Request, api *model.RuntimeApi) error

	// r：原生Request请求
	// api：根据当前请求，从配置文件中获取的Cancel的URL信息
	// nodes：Try 时可能成功的步骤，即需要回滚的步骤（根据Try返回值封装生成）
	// 返回值：
	// 	1、执行取消时，失败步骤的ID编号集合
	// 	2、错误信息
	Cancel(r *http.Request, api *model.RuntimeApi, nodes []*model.RuntimeTCC) ([]int64, error)
}

// 默认的处理逻辑
// 如果有和业务耦合无法剥离的情况，需要自定义处理
// 只要实现接口tcc的接口即可
type DefaultTcc struct {
}

func NewDefaultTcc() tcc {
	return &DefaultTcc{}
}



// 如果 err == nil ，则全部 api.nodes 都 try 成功，上层可以 commit 。
// 如果 err != nil ，则可能部分成功，需要回滚已经 try 成功的 success nodes 。

func (d *DefaultTcc) Try(r *http.Request, api *model.RuntimeApi) ([]*data.SuccessStep, error) {

	var success []*data.SuccessStep

    // 每个 node 执行 try 操作，直到遇到失败，返回已经成功 try 的 nodes 。
	for _, node := range api.Nodes {

		// 根据规则，由 r.RequestURI 和 node.Try.Url 构造请求 url
		tryURL := util.URLRewrite(api.UrlPattern, r.RequestURI[len(serverName)+1:], node.Try.Url)

		// 发送 Http 请求，执行 node 节点的 try 操作
		dt, err := util.HttpForward(tryURL, node.Try.Method, []byte(api.RequestInfo.Param), r.Header, time.Duration(node.Try.Timeout))

		// 不管成功与否（主要为了防止：当服务方接收并处理成功，但返回时失败），将结果保存起来，以备使用。
		// 如果插入失败，则直接返回，并在后续回滚之前的步骤
		ss := &data.SuccessStep{
			RequestId: api.RequestInfo.Id,
			Index:     node.Index,
			Url:       tryURL,
			Method:    node.Try.Method,
			Param:     string(api.RequestInfo.Param),
			Result:    string(dt),						 	// 执行结果，json（成功 or 失败）
			Status:    constant.RequestTypeTry,				// 请求类型
		}
		success = append(success, ss)

		// 这里有三种可能的失败
		// 1. http 请求失败（如超时），则失败，但该节点实际可能已经 try 成功，也需要当作 success 后续统一回滚。
		// 2. http 响应解析失败，认为失败，但是该节点是否成功未知，当作 success 后续统一回滚。
		// 3. http 明确反馈 try 失败，则当前节点无需回滚，不需要当作 success 返回，但是看实现也作为 success 返回了，但是设置了 ss.Resp 字段，
		//    在 cancel 之前可以检查一下。假如，对明确 try 失败的 node 的回滚会有什么问题呢，拓展一点，对于没有收到 try 的节点回滚，又会有什么问题呢?

		// http 请求失败，整体结束，返回已经成功执行的 step
		if err != nil {
			log.Errorf("access try method failed, error info: %s", err)
			return success, err
		}

		// http 结果无法解析，整体结束，返回已经成功执行的 step
		var rst *util.Response
		err = json.Unmarshal(dt, &rst)
		ss.Resp = rst
		if err != nil {
			return success, err
		}

		// 如果请求成功，但结果表示失败，整体结束，返回已经成功执行的 step
		if rst.Code != constant.Success {
			return success, fmt.Errorf(rst.Msg)
		}

		// 如果请求成功，结果也表示成功，继续执行下一个 step
	}
	return success, nil
}

func (d *DefaultTcc) Confirm(r *http.Request, api *model.RuntimeApi) error {


	// 遍历所有需要提交的 nodes
	for _, node := range api.Nodes {


		var rst *util.Response
		URL := util.URLRewrite(api.UrlPattern, r.RequestURI[len(serverName)+1:], node.Confirm.Url)

		// confirm
		dt, err := util.HttpForward(URL, node.Confirm.Method, []byte(api.RequestInfo.Param), r.Header, time.Duration(node.Confirm.Timeout))
		if err != nil {
			log.Errorf("confirm failed, please check it. error info is: %+v", err)
			return err
		}
		log.Infof("[%s] confirm response back content is: %+v", URL, string(dt))


		//
		err = json.Unmarshal(dt, &rst)
		if err != nil {
			log.Errorf("confirm failed, please check it. error info is: %+v", err)
			return err
		}

		if rst.Code != constant.Success {
			err = fmt.Errorf(rst.Msg)
			log.Errorf("confirm failed, please check it. error info is: %+v", err)
			return err
		}

		// 处理成功后，修改状态
		various.C.Confirm(api.RequestInfo.Id)
	}


	// 全部提交成功，则修改事务 Id 状态为 `提交成功`，避免重复调用
	various.C.UpdateRequestInfoStatus(constant.RequestInfoStatus1, api.RequestInfo.Id)


	return nil
}

func (d *DefaultTcc) Cancel(r *http.Request, api *model.RuntimeApi, nodes []*model.RuntimeTCC) ([]int64, error) {
	var ids []int64
	for _, node := range nodes {


		var rst *util.Response
		URL := util.URLRewrite(api.UrlPattern, r.RequestURI[len(serverName)+1:], node.Cancel.Url)

		// cancel
		dt, err := util.HttpForward(URL, node.Cancel.Method, []byte(api.RequestInfo.Param), r.Header, time.Duration(node.Cancel.Timeout))
		if err != nil {
			log.Errorf("cancel failed, please check it. error info is: %+v", err)
			return nil, err
		}
		log.Infof("[%s] cancel response back content is: %+v", URL, string(dt))

		err = json.Unmarshal(dt, &rst)
		if err != nil {
			return nil, err
		}

		if rst.Code != constant.Success {
			err = fmt.Errorf(rst.Msg)
			log.Errorf("cancel failed, please check it. error info is: %+v", err)
			return nil, err
		}

		// 如果当前数据有异常，则跳过此数据（交由后继异常流程处理）
		if node.SuccessStep.Id == 0 {
			continue
		}

		// 用于处理成功后，修改状态使用
		ids = append(ids, node.SuccessStep.Id)

	}
	return ids, nil
}
