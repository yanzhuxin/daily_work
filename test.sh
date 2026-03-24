#!/bin/bash
# save as: /usr/local/bin/opencode-session

START_TIME=$(date +%Y%m%d_%H%M%S)
REPORT_DIR="/home/yanzhuxin/opencode-reports"
REPO_DIR="/home/yanzhuxin/daily_work"  # 你的代码仓库路径

mkdir -p $REPORT_DIR

# 记录启动前已有的会话数量
PRE_SESSION_COUNT=$(opencode session list 2>/dev/null | wc -l)

# 启动 OpenCode 并捕获会话
echo "启动 OpenCode 会话 (开始时间: $START_TIME)"

# 启动真实的 opencode
opencode "$@"

EXIT_CODE=$?
END_TIME=$(date +%Y%m%d_%H%M%S)

echo "会话结束，正在获取会话信息..."

# 获取最新创建的会话 ID
# opencode session list 输出包含表头和分隔线，需要跳过前2行，取第一行实际会话
SESSION_ID=$(opencode session list 2>/dev/null | tail -n +3 | head -n 1 | awk '{print $1}')

# 如果获取失败，使用随机生成
if [ -z "$SESSION_ID" ] || [ "$SESSION_ID" = "Session" ]; then
    SESSION_ID=$(uuidgen | cut -d'-' -f1)
    echo "⚠️ 无法获取 opencode 会话 ID，使用随机生成: $SESSION_ID"
else
    echo "✅ 获取到 opencode 会话 ID: $SESSION_ID"
fi

echo "正在生成归档报告..."

# 生成 Markdown 报告
cat > "$REPORT_DIR/session_${START_TIME}_${SESSION_ID}.md" << EOF
# OpenCode 会话归档报告

## 元信息
- **会话 ID**: $SESSION_ID
- **开始时间**: $START_TIME
- **结束时间**: $END_TIME
- **工作目录**: $(pwd)
- **执行命令**: opencode $*

## 任务完成情况
[需手动填写或由 AI 生成]

## 文件变更
\`\`\`bash
$(cd $REPO_DIR && git status --short 2>/dev/null || echo "未在 git 仓库中")
\`\`\`

## 待办事项 (TODO)
- [ ] 

## 关键代码片段
\`\`\`
[会话中的重要代码或决策]
\`\`\`

## 备注
自动生成的会话归档
EOF

# 导出完整会话 JSON 数据
opencode export "$SESSION_ID" > "$REPORT_DIR/session_${START_TIME}_${SESSION_ID}.json" 2>/dev/null
if [ -f "$REPORT_DIR/session_${START_TIME}_${SESSION_ID}.json" ]; then
    cp "$REPORT_DIR/session_${START_TIME}_${SESSION_ID}.json" "$REPO_DIR/docs/sessions/"
    echo "✅ 已导出完整会话 JSON"
fi

# 复制报告到代码仓库
mkdir -p "$REPO_DIR/docs/sessions/"
cp "$REPORT_DIR/session_${START_TIME}_${SESSION_ID}.md" "$REPO_DIR/docs/sessions/"

# 自动提交到 Git
cd $REPO_DIR
if [ -d ".git" ]; then
    git add docs/sessions/
    git commit -m "chore: 归档 OpenCode 会话 $SESSION_ID ($START_TIME)"
    git push origin $(git branch --show-current)
    echo "✅ 已自动提交到仓库"
else
    echo "⚠️ 未找到 git 仓库，跳过提交"
fi

exit $EXIT_CODE
