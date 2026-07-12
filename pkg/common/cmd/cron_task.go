// 版权所有 © 2023 OpenIM。保留所有权利。
//
// 根据 Apache 许可证 2.0 版本（“许可证”）授权
// 除非符合许可证，否则不得使用此文件。
// 可在以下网址获取许可证副本：
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// 除非适用法律要求或书面同意，否则按“原样”分发该软件，
// 不附带任何明示或暗示的担保或条件。
// 请参阅许可证以了解具体语言规定和权限限制。

package cmd

import (
	"BaoIM-Server/internal/tools"    // 导入工具包
	"BaoIM-Server/pkg/common/config" // 导入公共配置包
	"github.com/spf13/cobra"         // 导入 cobra 命令行库
)

// CronTaskCmd 结构体定义定时任务命令
type CronTaskCmd struct {
	*RootCmd                                         // 继承 RootCmd 结构体
	initFunc func(config *config.GlobalConfig) error // 定义初始化函数，参数为全局配置，返回 error
}

// NewCronTaskCmd 创建新的 CronTaskCmd 实例
func NewCronTaskCmd() *CronTaskCmd {
	ret := &CronTaskCmd{RootCmd: NewRootCmd("cronTask", WithCronTaskLogName()), // 初始化 RootCmd，并设置日志名称
		initFunc: tools.StartTask} // 初始化函数为 tools.StartTask
	ret.addRunE()         // 添加 RunE 方法
	ret.SetRootCmdPt(ret) // 设置 RootCmd 指针
	return ret            // 返回新建对象
}

// addRunE 给命令添加 RunE 执行函数
func (c *CronTaskCmd) addRunE() {
	c.Command.RunE = func(cmd *cobra.Command, args []string) error { // 设置命令运行时执行的函数
		return c.initFunc(c.config) // 执行初始化函数，传入配置
	}
}

// Exec 执行命令
func (c *CronTaskCmd) Exec() error {
	return c.Execute() // 调用父类的 Execute 方法
}

// GetPortFromConfig 根据端口类型从配置获取端口（此处始终返回 0）
func (c *CronTaskCmd) GetPortFromConfig(portType string) int {
	return 0 // 返回 0
}
