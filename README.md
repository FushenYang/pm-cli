# pm-cli


## 命令正常使用

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
