# pm-cli


## 命令正常使用


这是下载所有数据的的自命令。
``` bash

pnpm dev all
```

## agent相关

本项目使用了effect.ts，需要时可以更新源代码供ai参考

第一次部署运行了：
``` bash
git subtree add --prefix=repos/effect https://github.com/Effect-TS/effect.git main --squash
```

后续如果想更新，运行这个命令
``` bash
git subtree pull --prefix=repos/effect https://github.com/Effect-TS/effect.git main --squash
```

另外，因为配置的目录忽略，所以vs正常看不到repos目录，但是agent.md会提醒copilot。

# duckdb使用技巧

## 加载环境变量

`export $(cat .env | xargs)`


## 运行方式

改进了运行方式，以下两种方式都是可以的。添加参数会找到对应的脚本，不添加参数系统会自己找最新的修改。
`pnpm db`
`pnpm db query`

