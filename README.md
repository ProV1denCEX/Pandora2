## Common Kit

这是一个用来存放团队常用小工具的库，任何团队成员均可直接在master分支上进行增删改查。因此，其他成员使用时，可能需要进行针对性的调试。

### 使用说明
1. 拷贝`common.py`,`helper.py`,`settings.ini.template`文件到新项目或现有项目的任意目录中
2. 执行`common.py`中的`main`函数确认当前目录结构正确
3. (可选) 取消注释 `common.py` 276行附近的 `Settings = Configs.parse()` 代码

### 依赖关系
> 如需修改, 注意**不要构成相互引用**
`helper.py > common.py`

### helper模块

- 数据库管理
    - CRUD操作
    - 对dataframe的upsert操作
- 邮件收发
    - MailSender
    - MailReceiver
- 交易日管理
    - TDays
    - 判断、加减、区间
- 自然日管理
    - Dates
    - 加减、解析、转换

### common 模块

- 提供无三方依赖的底层函数实现
  - 常量
  - 常用类型
  - 配置文件模板
  - 配置文件解析
  - 团队成员联系方式
  - 日志工具
  - 配置文件映射到实例