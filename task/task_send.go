package task

import (
	"fmt"
	"strings"
	"github.com/blastbao/tcc/constant"
	"github.com/blastbao/tcc/global/config"
	"github.com/blastbao/tcc/global/various"
	"github.com/blastbao/tcc/send"
	"github.com/blastbao/tcc/send/email"
	"github.com/blastbao/tcc/store/data"
)

func taskToSend(needRollbackData []*data.RequestInfo, subject string) {
	var s send.Send = email.NewEmailSender(*config.EmailUsername, subject, strings.Split(*config.EmailTo, ","))
	for _, v := range needRollbackData {
		// 如果重试次数已超过阈值，且尚未发送过 email，则需要发送 email
		if v.Times >= constant.RetryTimes && v.IsSend != constant.SendSuccess {
			err := s.Send([]byte(fmt.Sprintf("this data is wrong, please check it. information: %+v", v)))
			if err == nil {
				// 发送成功，更新 v.IsSend 为 constant.SendSuccess
				various.C.UpdateRequestInfoSend(v.Id)
			}
		}
	}

	// 如果失败的过多，发告警邮件
	if len(needRollbackData) > *config.MaxExceptionalData {
		s.Send([]byte(fmt.Sprintf("The exceptional data is too much [%d], please check it.", len(needRollbackData))))
	}
}
