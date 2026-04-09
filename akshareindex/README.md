# AKShare 文档双索引包

本目录保留了下载下来的 AKShare 在线文档原始文件，并额外生成了两套索引，便于模型或脚本按层级、按接口进行定位。

## 目录结构

- `raw/site/`：原始站点文件镜像，文件名和相对路径保持原文结构
- `index/pages.jsonl`：按页面索引，一行一个页面
- `index/function_map.jsonl`：按接口索引，一行一个接口记录
- `index/toc.json`：按文档层级生成的目录树
- `index/manifest.json`：整体统计与使用说明

## 查询建议

### 先按页面找
当需求是“我要找某个主题在哪一页”时，先查 `index/pages.jsonl`：
- `page_title`
- `breadcrumbs`
- `headings`
- `function_names`
- `preferred_content_path`

### 再按接口找
当需求是“我要调用哪个 AKShare 接口”时，查 `index/function_map.jsonl`：
- `function_name`
- `section_title`
- `description`
- `target_url`
- `input_names`
- `output_names`
- `source_relpath`

## 字段约定

- `function_name_exact`：接口英文名，保持原文一致
- `section_title_exact`：接口所在中文小节标题，保持原文一致
- `description_exact`：接口“描述”字段，保持原文一致
- `source_relpath` / `html_relpath`：都指向本目录内的真实文件
- `preferred_content_path`：优先读取的原文路径，通常是 `_sources` 源码

## 说明

1. 本包没有精简原文内容，索引只做定位，不改写接口命名。
2. 页面索引与接口索引均保留中文名和英文名的原文写法。
3. 个别工具页（如 `search.html`、`genindex.html`、`_downloads.html`）没有源码页，在索引里已标记为 `is_utility_page=true`。
