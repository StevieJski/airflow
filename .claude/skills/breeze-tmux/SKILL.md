---
name: breeze-tmux
description: Monitor and interact with Breeze tmux sessions for Airflow logs. Use this skill when you need to capture output from breeze start-airflow tmux panes.
argument-hint: [operation] [pane]
allowed-tools: Bash
---

# Breeze tmux Session Monitoring

When `breeze start-airflow` is running, it creates a tmux session with multiple panes for Airflow components. Use these commands to monitor without blocking.

## IMPORTANT: Prefer Non-tmux Alternatives

Before using tmux monitoring, consider these simpler approaches:

```bash
# Check service health directly
breeze exec "airflow jobs check"

# View scheduler logs
breeze exec "cat $AIRFLOW_HOME/logs/scheduler/latest/*.log | tail -100"

# View webserver logs
breeze exec "cat $AIRFLOW_HOME/logs/webserver/latest/*.log | tail -100"

# Check DAG status
breeze exec "airflow dags list-runs -d example_bash_operator"
```

## tmux Session Basics

### List tmux sessions
```bash
tmux list-sessions
```

### List panes in Airflow session
```bash
tmux list-panes -t airflow
```

### Check if Airflow tmux session exists
```bash
tmux has-session -t airflow 2>/dev/null && echo "Running" || echo "Not running"
```

## Capture Pane Output (Non-Interactive)

### Capture current pane content
```bash
# Capture last 100 lines from default pane
tmux capture-pane -t airflow -p | tail -100

# Capture specific pane (0, 1, 2, etc.)
tmux capture-pane -t airflow:0.0 -p | tail -50  # Scheduler
tmux capture-pane -t airflow:0.1 -p | tail -50  # Webserver
tmux capture-pane -t airflow:0.2 -p | tail -50  # Triggerer
```

### Capture with history (more lines)
```bash
# Capture with scrollback buffer
tmux capture-pane -t airflow -p -S -500 | tail -200
```

### Capture to file for analysis
```bash
tmux capture-pane -t airflow -p -S -1000 > /tmp/airflow-logs.txt
```

## Search for Errors in tmux Output

### Check for errors
```bash
tmux capture-pane -t airflow -p -S -500 | grep -i "error\|exception\|failed"
```

### Check for warnings
```bash
tmux capture-pane -t airflow -p -S -500 | grep -i "warning\|warn"
```

### Check for specific DAG issues
```bash
tmux capture-pane -t airflow -p -S -500 | grep -i "dag_id"
```

## Send Commands to tmux (Use with Caution)

### Send a command to a pane
```bash
# Send command to scheduler pane
tmux send-keys -t airflow:0.0 "airflow dags list" Enter

# Wait and capture output
sleep 2
tmux capture-pane -t airflow:0.0 -p | tail -30
```

### Send Ctrl+C to stop a process
```bash
tmux send-keys -t airflow:0.0 C-c
```

## Monitor Continuously (Background)

### Tail tmux output to file
```bash
# Start background monitoring
while true; do
  tmux capture-pane -t airflow -p >> /tmp/airflow-monitor.log
  sleep 5
done &
```

### Watch for specific events
```bash
# One-shot check for task completion
tmux capture-pane -t airflow -p -S -100 | grep -q "Task exited with return code 0" && echo "Task completed"
```

## Typical Airflow tmux Pane Layout

When using `breeze start-airflow`, panes are typically:
- **Pane 0**: Scheduler
- **Pane 1**: Webserver
- **Pane 2**: Triggerer (if enabled)
- **Pane 3**: Worker (if using CeleryExecutor)

```bash
# Verify pane layout
tmux list-panes -t airflow -F "#{pane_index}: #{pane_title}"
```

## Kill tmux Session

### Stop Airflow tmux session
```bash
tmux kill-session -t airflow
```

### Stop all tmux sessions
```bash
tmux kill-server
```

## Automation Pattern: Start and Monitor

```bash
#!/bin/bash
# Start Airflow in background
breeze start-airflow &
BREEZE_PID=$!

# Wait for tmux session to appear
for i in {1..30}; do
  if tmux has-session -t airflow 2>/dev/null; then
    echo "Airflow tmux session started"
    break
  fi
  sleep 2
done

# Monitor for errors
sleep 30  # Let services start
ERRORS=$(tmux capture-pane -t airflow -p -S -200 | grep -ci "error\|exception")
if [ "$ERRORS" -gt 0 ]; then
  echo "Found $ERRORS errors in Airflow logs"
  tmux capture-pane -t airflow -p -S -200 | grep -i "error\|exception"
fi
```

## Tips
- **Always prefer `breeze exec` over tmux interaction** for automation
- `capture-pane` is non-blocking and agent-safe
- Avoid `tmux attach` - it blocks the agent
- Use `-S -N` to capture N lines of scrollback history
- Pane indices may vary based on executor configuration
- Kill the tmux session with `breeze down` or `tmux kill-session`
- For real-time monitoring, capture periodically rather than attaching
- Consider using Airflow's REST API for status checks instead of log scraping
