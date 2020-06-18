package main

import (
	"fmt"
	"net/http"
	"github.com/blastbao/tcc/constant"
	"github.com/blastbao/tcc/global/various"
	"github.com/blastbao/tcc/log"
	"github.com/blastbao/tcc/model"
	"github.com/blastbao/tcc/store/data"
	"github.com/blastbao/tcc/util"
)

type proxy struct {
	t tcc
}

// 处理 http 请求
func (p *proxy) process(writer http.ResponseWriter, request *http.Request) {
	// 响应构造
	var response = &util.Response{}

	// 参数解析
	params := util.GetParams(request)
	log.Infof("welcome to tcc. url is %s, and param is %s", request.RequestURI, string(params))


	// 将请求信息持久化，失败则报错
	ri := &data.RequestInfo{
		Url:    request.RequestURI[len(serverName)+1:],
		Method: request.Method,
		Param:  string(params),
	}
	err := various.C.InsertRequestInfo(ri)
	if err != nil {
		response.Code = constant.InsertTccDataErrCode
		response.Msg = err.Error()
		util.ResponseWithJson(writer, response)
		log.Errorf("program have a bug, please check it. error info: %s", err)
		return
	}

	// 根据请求路径 url ，确定对应的一组 tcc 对象，每个对象包含 try、confirm/cancel 行为
	runtimeAPI, err := various.GetApiWithURL(request.RequestURI[len(serverName)+1:])
	if err != nil {
		response.Code = constant.NotFoundErrCode
		response.Msg = err.Error()
		util.ResponseWithJson(writer, response)
		log.Warnf("there is no request info in configuration")
		return
	}
	runtimeAPI.RequestInfo = ri

	// 对每个 runtimeAPI.Nodes 执行 try 操作，若发送错误，返回需要回滚的 nodes(cancelSteps) 。
	cancelSteps, err := p.try(request, runtimeAPI)

	// 如果出错，意味着需要回滚
	if err != nil {
		// 回滚 success 的 nodes
		if len(cancelSteps) > 0 {
			go p.cancel(request, runtimeAPI, cancelSteps)
		}
		log.Errorf("try failed, error info is: %s", err)
		// 返回失败给 client
		response.Code = constant.InsertTccDataErrCode
		response.Msg = err.Error()
		util.ResponseWithJson(writer, response)
		return
	}

	// 没有出错，所有 Nodes 的 try 成功，提交事务
	go p.confirm(request, runtimeAPI)

	// 返回成功
	response.Code = constant.Success
	util.ResponseWithJson(writer, response)
	return
}

// 对每个 api.Nodes 执行 try 操作，若发送错误，返回需要回滚的 nodes 。
func (p *proxy) try(r *http.Request, api *model.RuntimeApi) ([]*model.RuntimeTCC, error) {

	// 参数检查
	if len(api.Nodes) == 0 {
		return nil, fmt.Errorf("no method need to execute")
	}

	// 对每个 api.Nodes 执行 try 操作，直到遇到首个失败结束，返回已经成功 Try 的 nodes 。
	success, err := p.t.Try(r, api)

	// 将成功 Try 的 nodes 信息批量存入数据库
	err2 := various.C.BatchInsertSuccessStep(success)

	// 这块有两种失败：
	// 1. p.t.Try() 失败，则意味着有某些 nodes 的 Try 失败，对于成功的 success nodes 需要回滚。
	// 2. 将 success 更新到数据库失败，则这些 success nodes 需要回滚。
	var nextCancelStep []*model.RuntimeTCC
	if err != nil || err2 != nil {
		// 将 success 的 nodes 保存起来，以便后续回滚
		for _, node := range api.Nodes {
			for _, s := range success {
				if node.Index == s.Index {
					node.SuccessStep = s
					nextCancelStep = append(nextCancelStep, node)
				}
			}
		}
		// 返回这些需要回滚的 nodes
		return nextCancelStep, err
	}

	//// !!! 不可能走到这里吧???
	//if err2 != nil {
	//	return nextCancelStep, err2
	//}

	// 如果均无失败，可能是一种异常? 感觉走不到这里
	if len(success) == 0 {
		return nil, fmt.Errorf("no successful method of execution")
	}

	// 返回需要回滚的 nodes
	return nextCancelStep, nil
}


func (p *proxy) confirm(r *http.Request, api *model.RuntimeApi) error {

	err := p.t.Confirm(r, api)
	if err != nil {
		// 提交失败（需要继续提交）
		various.C.UpdateRequestInfoStatus(constant.RequestInfoStatus2, api.RequestInfo.Id)
		return err
	}

	// 提交成功
	various.C.Confirm(api.RequestInfo.Id)
	// 提交成功
	various.C.UpdateRequestInfoStatus(constant.RequestInfoStatus1, api.RequestInfo.Id)
	return nil
}

func (p *proxy) cancel(r *http.Request, api *model.RuntimeApi, nodes []*model.RuntimeTCC) error {

	ids, err := p.t.Cancel(r, api, nodes)
	if err != nil {
		various.C.UpdateRequestInfoStatus(constant.RequestInfoStatus4, api.RequestInfo.Id)
		return err
	}

	for _, id := range ids {
		various.C.UpdateSuccessStepStatus(api.RequestInfo.Id, id, constant.RequestTypeCancel)
	}
	various.C.UpdateRequestInfoStatus(constant.RequestInfoStatus3, api.RequestInfo.Id)
	return nil
}
