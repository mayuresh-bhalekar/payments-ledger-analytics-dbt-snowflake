#!/usr/bin/env bash
# Verify the whole local dbt environment chain, in the order it actually breaks.
#
# Written after an hour lost to the dbt Power User extension reporting
# "dbt not found" when dbt was running fine — the real fault was DBT_PROFILES_DIR
# pointing at an unrelated project. Every check below corresponds to a failure
# actually hit, not a hypothetical one.
#
# Usage:  ./scripts/check_dbt_env.sh [project_dir]     (default: ./dbt_project)
# Exit 0 = all good. Non-zero = first failing check is printed with the fix.

set -uo pipefail

PROJECT_DIR="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/dbt_project}"
REPO_ROOT="$(cd "$(dirname "$PROJECT_DIR")" && pwd)"
FAILED=0

pass() { printf "  \033[32mOK\033[0m   %s\n" "$1"; }
fail() { printf "  \033[31mFAIL\033[0m %s\n" "$1"; FAILED=1; }
info() { printf "       %s\n" "$1"; }

echo "dbt environment check: $REPO_ROOT"
echo

# --- 1. No tilde interpreter paths -------------------------------------------
# VS Code does NOT expand "~" in interpreter paths. A tilde fails to resolve and
# silently falls back to system Python.
echo "1. VS Code interpreter path"
SETTINGS="$REPO_ROOT/.vscode/settings.json"
if [ ! -f "$SETTINGS" ]; then
  info "no .vscode/settings.json (fine for CLI-only use)"
elif grep -q '"~/' "$SETTINGS"; then
  fail "tilde path in .vscode/settings.json — VS Code cannot expand '~'"
  info "fix: use an absolute path or \${workspaceFolder}"
else
  pass "no tilde paths"
fi

# --- 2. Interpreter can actually RUN dbt -------------------------------------
# Python 3.14 has dbt installed but crashes on import (mashumaro
# UnserializableField). The extension renders that crash as "dbt not found",
# so importing is not enough — it must run.
echo "2. dbt actually runs on the pinned interpreter"
PY=""
if [ -f "$SETTINGS" ]; then
  PY=$(python3 -c "
import json,re,sys
s=open('$SETTINGS').read()
s=re.sub(r'^\s*//.*$','',s,flags=re.M)
d=json.loads(s)
p=d.get('dbt.dbtPythonPathOverride') or d.get('python.defaultInterpreterPath') or ''
print(p.replace('\${workspaceFolder}','$REPO_ROOT'))
" 2>/dev/null)
fi
[ -z "$PY" ] && PY="$(command -v python3)"

if [ ! -x "$PY" ]; then
  fail "interpreter not executable: $PY"
else
  VER=$("$PY" -c "from dbt.cli.main import dbtRunner; import dbt.version; print(dbt.version.__version__)" 2>&1 | tail -1)
  if [[ "$VER" =~ ^[0-9]+\.[0-9]+ ]]; then
    pass "dbt $VER via $PY"
  else
    fail "dbt cannot run on $PY"
    info "$VER"
    info "fix: point at a venv whose Python is 3.12 (3.14 breaks dbt's mashumaro dep)"
  fi
fi

# --- 3. DBT_PROFILES_DIR -----------------------------------------------------
# ~/.zshrc auto-switches this per directory. It always wins over a local
# profiles.yml, so a wrong value here breaks everything downstream while dbt
# itself looks perfectly healthy.
echo "3. DBT_PROFILES_DIR"
RESOLVED=$(cd "$REPO_ROOT" && zsh -i -c 'echo $DBT_PROFILES_DIR' 2>/dev/null | tail -1)
if [ -z "$RESOLVED" ]; then
  info "unset — dbt will use its ~/.dbt default"
  RESOLVED="$HOME/.dbt"
elif [ ! -d "$RESOLVED" ]; then
  fail "resolves to a non-existent dir: $RESOLVED"
else
  pass "resolves to $RESOLVED"
fi

# --- 4. The profile this project needs exists where dbt will look ------------
echo "4. profile lookup"
PROFILE=$(grep -E "^profile:" "$PROJECT_DIR/dbt_project.yml" 2>/dev/null | head -1 | sed "s/profile:[[:space:]]*//; s/['\"]//g" | tr -d '\r')
if [ -z "$PROFILE" ]; then
  fail "could not read 'profile:' from $PROJECT_DIR/dbt_project.yml"
elif grep -q "^${PROFILE}:" "$RESOLVED/profiles.yml" 2>/dev/null; then
  pass "profile '$PROFILE' found in $RESOLVED/profiles.yml"
else
  fail "profile '$PROFILE' NOT in $RESOLVED/profiles.yml"
  info "this is the exact fault behind 'Could not find profile named ...'"
  info "fix: add it there, or correct the DBT_PROFILES_DIR case in ~/.zshrc"
fi

# --- 5. End to end -----------------------------------------------------------
# Uses the same entrypoint the extension does (dbtRunner), so a pass here means
# the extension will work too. Sources the project's .env first, because
# profiles.yml is full of env_var() calls that won't render without it — same
# file the extension picks up via python.envFile.
echo "5. dbt parse (same entrypoint the VS Code extension uses)"
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a; . "$PROJECT_DIR/.env" >/dev/null 2>&1; set +a
  info "sourced $(basename "$PROJECT_DIR")/.env"
fi
if [ -x "$PY" ]; then
  OUT=$("$PY" -c "
from dbt.cli.main import dbtRunner
r = dbtRunner().invoke(['parse','--project-dir','$PROJECT_DIR','--profiles-dir','$RESOLVED'])
print('SUCCESS' if r.success else 'FAILED: %s' % r.exception)
" 2>/dev/null | tail -1)
  if [ "$OUT" = "SUCCESS" ]; then
    pass "project parses"
  else
    fail "$OUT"
  fi
fi

echo
if [ "$FAILED" -eq 0 ]; then
  printf "\033[32mAll checks passed.\033[0m\n"
else
  printf "\033[31mSomething is broken — see FAIL above.\033[0m\n"
  echo "Extension's own log (trust it over any UI banner):"
  echo '  ls -t ~/Library/Application\ Support/Code/logs/*/window*/exthost/innoverio.vscode-dbt-power-user/Log\ -\ dbt.log | head -1'
fi
exit "$FAILED"
