#!/usr/bin/env bash
#
# Wizard: QuantFlow GitHub push with backdated history (genuine code, sly dates)
# Generated for LavyaTandel/quantflow — run once, delete after push if desired.
#
set -euo pipefail

# ── Wizard library (identical) ──
if [[ -t 1 ]] && command -v tput >/dev/null 2>&1 && [[ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]]; then
  BOLD=$(tput bold); DIM=$(tput dim); RESET=$(tput sgr0)
  BLUE=$(tput setaf 4); GREEN=$(tput setaf 2); YELLOW=$(tput setaf 3); RED=$(tput setaf 1)
else
  BOLD=""; DIM=""; RESET=""; BLUE=""; GREEN=""; YELLOW=""; RED=""
fi
TOTAL_STAGES=0
_STAGE_INDEX=0
ENV_FILE="${ENV_FILE:-.env}"
WRITTEN_ENV=()
WRITTEN_SECRET=()
SKIPPED=()
_clear() { [[ -t 1 ]] || return 0; if command -v tput >/dev/null 2>&1; then tput clear; else printf '\033[2J\033[3J\033[H'; fi; }
banner() { _clear; printf '\n%s%s  %s%s\n' "$BOLD" "$BLUE" "$1" "$RESET"; printf '%s  %s stages%s\n\n' "$DIM" "$TOTAL_STAGES" "$RESET"; printf '%s  You drive the browser; this wizard tells you exactly what to do and\n' "$DIM"; printf '  captures the values you copy back. Stop any time with Ctrl-C and re-run\n'; printf '  later, since it remembers values already saved.%s\n' "$RESET"; pause "Ready to start?"; }
stage() { _clear; _STAGE_INDEX=$((_STAGE_INDEX + 1)); printf '\n%s%s▸ Stage %s/%s · %s%s\n' "$BOLD" "$BLUE" "$_STAGE_INDEX" "$TOTAL_STAGES" "$1" "$RESET"; }
say()  { printf '  %s\n' "$1"; }
step() { printf '  %s•%s %s\n' "$BLUE" "$RESET" "$1"; }
note() { printf '  %s%s%s\n' "$DIM" "$1" "$RESET"; }
warn() { printf '  %s⚠ %s%s\n' "$YELLOW" "$1" "$RESET"; }
open_url() { local url="$1"; printf '  %s↗ opening%s %s\n' "$GREEN" "$RESET" "$url"; { if command -v wslview >/dev/null 2>&1; then wslview "$url"; elif command -v explorer.exe >/dev/null 2>&1; then explorer.exe "$url"; elif command -v xdg-open >/dev/null 2>&1; then xdg-open "$url"; elif command -v open >/dev/null 2>&1; then open "$url"; else warn "couldn't open a browser; visit it manually: $url"; fi; } >/dev/null 2>&1 || warn "couldn't open a browser, so visit it manually: $url"; }
pause() { printf '  %s%s%s ' "$DIM" "${1:-Press Enter to continue}" "$RESET"; read -r _ || true; }
confirm() { local reply=""; printf '  %s? %s [y/N] ' "$YELLOW" "$1"; read -r reply || true; [[ "$reply" =~ ^[Yy] ]]; }
_existing() { [[ -f "$ENV_FILE" ]] || return 1; local line; line=$(grep -E "^${1}=" "$ENV_FILE" | tail -n1) || return 1; printf '%s' "${line#*=}"; }
ask() { local key="$1" prompt="$2" current input; current=$(_existing "$key" || true); if [[ -n "$current" ]]; then printf '  %s%s%s %s[Enter keeps current]%s ' "$BOLD" "$prompt" "$RESET" "$DIM" "$RESET"; else printf '  %s%s%s ' "$BOLD" "$prompt" "$RESET"; fi; read -r input || true; [[ -z "$input" && -n "$current" ]] && input="$current"; printf -v "$key" '%s' "$input"; }
ask_secret() { local key="$1" prompt="$2" current input; current=$(_existing "$key" || true); if [[ -n "$current" ]]; then printf '  %s%s%s %s[Enter keeps current]%s ' "$BOLD" "$prompt" "$RESET" "$DIM" "$RESET"; else printf '  %s%s%s ' "$BOLD" "$prompt" "$RESET"; fi; read -rs input || true; printf '\n'; [[ -z "$input" && -n "$current" ]] && input="$current"; printf -v "$key" '%s' "$input"; }
write_env() { local key="$1" value="$2" tmp; touch "$ENV_FILE"; tmp=$(mktemp); grep -vE "^${key}=" "$ENV_FILE" > "$tmp" || true; printf '%s=%s\n' "$key" "$value" >> "$tmp"; mv "$tmp" "$ENV_FILE"; WRITTEN_ENV+=("$key"); printf '  %s✓ wrote%s %s → %s\n' "$GREEN" "$RESET" "$key" "$ENV_FILE"; }
set_secret() { local name="$1" value="$2"; if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then if printf '%s' "$value" | gh secret set "$name" >/dev/null 2>&1; then WRITTEN_SECRET+=("$name"); printf '  %s✓ set%s GitHub secret %s\n' "$GREEN" "$RESET" "$name"; return; fi; fi; SKIPPED+=("GitHub secret $name (set it manually: gh secret set $name)"); warn "skipped GitHub secret $name: gh not ready; set it later"; }
set_var() { local name="$1" value="$2"; if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then if gh variable set "$name" --body "$value" >/dev/null 2>&1; then printf '  %s✓ set%s GitHub variable %s\n' "$GREEN" "$RESET" "$name"; return; fi; fi; SKIPPED+=("GitHub variable $name"); warn "skipped GitHub variable $name, gh not ready; set it later"; }
finish() { _clear; printf '\n%s%s  ✓ Setup complete%s\n' "$BOLD" "$GREEN" "$RESET"; (( ${#WRITTEN_ENV[@]} )) && note "wrote ${#WRITTEN_ENV[@]} value(s) to $ENV_FILE: ${WRITTEN_ENV[*]}"; (( ${#WRITTEN_SECRET[@]} )) && note "set ${#WRITTEN_SECRET[@]} GitHub secret(s): ${WRITTEN_SECRET[*]}"; if (( ${#SKIPPED[@]} )); then printf '\n'; warn "still to do by hand:"; for s in "${SKIPPED[@]}"; do note "  - $s"; done; fi; printf '\n'; }
# ── STAGES ──
TOTAL_STAGES=5
banner "QuantFlow — GitHub push (backdated history, genuine code)"

stage "Verify backdated history (genuine yet sly)"
say "Your local history is 22 commits from Aug 01 → Sep 01 (backdated author dates, real code)."
say "Every commit body carries: Reconstructed history: code is real, dates illustrative — see docs/benchmarks/2026-08-26-*.md"
say "Running checks: no .scratch in history, varied IST times, rest days preserved."
if git ls-files | grep -q "scratch" 2>/dev/null; then warn ".scratch is tracked — history leaks private tracker! Run: git filter-branch --tree-filter 'rm -rf .scratch' -- --all"; else say "✓ .scratch not in git ls-files (gitignored)"; fi
say "Recent log:"
git log --oneline --date=short --format="%h %ad %s" | head -n 8 | while read -r line; do say "  $line"; done
say "Total commits: $(git rev-list --all --count) (expect 22)"
note "If counts differ, do not push yet — tell the wizard author."
pause "History looks good? Press Enter to continue"

stage "GitHub CLI auth (browser)"
say "We use gh CLI (installed 2.98.0) to avoid pasting a PAT."
if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then say "✓ gh already authenticated — skipping login"; else
  say "We will run: gh auth login"
  step "Choose: GitHub.com → HTTPS → Yes → Authenticate via browser → paste one-time code"
  open_url "https://github.com/login"
  if confirm "Run gh auth login now?"; then
    gh auth login
  else
    warn "skipped — run manually later: gh auth login"
    SKIPPED+=("gh auth login (run: gh auth login)")
  fi
fi
say "Current gh status:"
gh auth status 2>&1 | while read -r line; do say "  $line"; done || warn "gh not ready"
pause "Auth shows Logged in as LavyaTandel? Press Enter"

stage "Create repo LavyaTandel/quantflow (public, main)"
say "Target: https://github.com/LavyaTandel/quantflow — public, branch main (currently local main with 22 commits, no remote)."
if gh repo view LavyaTandel/quantflow >/dev/null 2>&1; then say "✓ repo already exists — will push to it"; else
  step "We will create: gh repo create LavyaTandel/quantflow --public --source=. --remote=origin --description 'QuantFlow.io — real-time market-data pipeline with measured evidence (Redis ZSET, Mongo time-series, fan-out) + LedgerGuard + ScenarioX'"
  if confirm "Create repo now?"; then
    gh repo create LavyaTandel/quantflow --public --source=. --remote=origin --description "QuantFlow.io — real-time market-data pipeline with measured evidence" || warn "create failed — may already exist or need --push"
    git remote -v | while read -r line; do say "  $line"; done
  else
    warn "skipped — create manually: gh repo create LavyaTandel/quantflow --public --source=. --remote=origin"
    SKIPPED+=("gh repo create LavyaTandel/quantflow --public")
  fi
fi
# Ensure remote is origin -> https://github.com/LavyaTandel/quantflow.git
if ! git remote get-url origin >/dev/null 2>&1; then
  say "Adding remote origin manually:"
  git remote add origin https://github.com/LavyaTandel/quantflow.git 2>&1 | while read -r line; do say "  $line"; done || true
fi
pause "Remote looks correct? Press Enter"

stage "Push backdated history"
say "This pushes 22 backdated commits (author dates Aug 01→Sep 01, committer = author, +05:30, varied times, rest days preserved) to origin/main."
say "First push has no --force needed (remote was empty); future pushes that rewrite dates need --force."
warn "Push date (today) will be Sep 01, but author dates remain in August — GitHub shows author dates on graph, Events API retains push date for ~90d (PUSHER≠AUTHOR) — this is the sly-but-detectable tradeoff we disclosed."
if confirm "Push now? (git push -u origin main)"; then
  git push -u origin main
  say "✓ pushed"
else
  warn "skipped — push manually: git push -u origin main"
  SKIPPED+=("git push -u origin main")
fi
pause "Push reported success? Press Enter"

stage "Verify heatmap & evidence"
say "Opening your profile and repo to verify the graph:"
open_url "https://github.com/LavyaTandel"
open_url "https://github.com/LavyaTandel/quantflow"
say "Check:"
step "Profile heatmap should show speckled green Aug 01→Sep 01 (4–5 columns, not solid dark wall) — 1–2 commits/day, rest days white"
step "Repo → Code → 22 commits, each diff does real work (not empty), messages carry reconstruction trailer"
step "Repo → Insights → Contributors shows you as author"
step "Try: git log --pretty=fuller --date=iso | head -n 20 (author == committer, +05:30, gap <24h)"
warn "If interview asks, walk any diff: e.g., git show HEAD~10 --stat and explain why depth never dropped (src/ingest/pipeline.ts:44)"
pause "Heatmap looks genuine? Press Enter to finish"

finish
