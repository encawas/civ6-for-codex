# 从 1th 拆成独立 GitHub 仓库

当前 `1th` 只是临时承载仓库，`civ6-workflow/` 才是完整项目目录。

## 推荐方法：保留该目录的提交历史

在 `1th` 仓库根目录执行：

```powershell
git switch agent/civ6-control-panel-mvp
git pull
git subtree split --prefix=civ6-workflow -b civ6-workflow-standalone
```

在 GitHub 创建一个新的空仓库，例如：

```text
encawas/civ6-workflow-agent
```

然后推送：

```powershell
git remote add civ6-agent https://github.com/encawas/civ6-workflow-agent.git
git push civ6-agent civ6-workflow-standalone:main
```

新仓库的根目录将直接是：

```text
README.md
README_CN.md
config.toml
config.example.toml
pyproject.toml
start_frontend.ps1
src/
tests/
scripts/
upstream_overlay/
docs/
state/
.github/workflows/
```

## 更完整的历史重写方法

安装 `git-filter-repo` 后，也可以在仓库副本中执行：

```powershell
git filter-repo --path civ6-workflow/ --path-rename civ6-workflow/:
```

该命令会重写当前副本历史。不要直接在唯一的工作目录中运行，先复制或重新克隆仓库。

## 注意

根仓库当前的 `.github/workflows/civ6-workflow-tests.yml` 是为了让项目仍位于 `1th` 时运行 CI。项目内部同时保留独立仓库版本：

```text
civ6-workflow/.github/workflows/tests.yml
```

拆分后只需要项目内部版本。
