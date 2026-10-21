# Migrated Role Permissions

## Purpose

Read the column for the assigned role before acting. This preserves all source permission entries as behavioral policy; it is not a Codex permission configuration or an operating-system sandbox.

## Interpretation

- The parent sandbox, approvals, actual tool availability, higher-priority instructions and current user authorization always apply. An `allow` cell grants no new host permission.
- Role scope and workflow prohibitions apply before tool allowances. Never bypass edit restrictions through shell, alternate tools or delegation.
- `ask` requires authorization for that operation unless already explicitly supplied. `deny` prohibits the operation. Prefer a non-destructive equivalent; report a real blocker rather than silently expanding permissions.
- Source glob syntax and tool names below are retained for traceability, not executed as rules. Treat narrower command patterns before broad wildcards; where equally specific entries conflict, use deny before ask before allow. The source did not define a portable precedence algorithm.
- Source read allowances for credential files do not authorize printing secrets. Kilo edit allowances are constrained by AGENTS.md's preservation rule.
- Docker, Kubernetes, database and frontend command families are conditional legacy allowances, not prerequisites for this Python CLI. Do not provision absent infrastructure.
- `agent_manager` and model-management denials prohibit changing agent definitions/model policy from an executor. `task` allowances do not make implementor or test-engineer orchestrators.
- Native delegation, search, filesystem and progress tools replace Kilo task/grep/glob/todo tools as available. Missing AST-editor/morfx tools remain prohibited. docs-specialist's MCP denial is behavioral because standalone agent files inherit MCP servers.

## Permission Matrix

A dash means no explicit source entry; consult the role's default wildcard and scope.

| Surface / source pattern | analyst | auditor | docs-specialist | implementor | planner | researcher | tech-lead | test-engineer | validator |
|---|---|---|---|---|---|---|---|---|---|
| `agent_manager: (default)` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `agent_manager_models: (default)` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `ast-editor_add_field: (default)` | — | — | — | deny | — | — | — | deny | — |
| `ast-editor_add_key: (default)` | — | — | — | deny | — | — | — | deny | — |
| `ast-editor_append_to_array: (default)` | — | — | — | deny | — | — | — | deny | — |
| `ast-editor_insert_in_body: (default)` | — | — | — | deny | — | — | — | deny | — |
| `ast-editor_insert_sibling: (default)` | — | — | — | deny | — | — | — | deny | — |
| `ast-editor_remove_from_array: (default)` | — | — | — | deny | — | — | — | deny | — |
| `ast-editor_replace_docstring: (default)` | — | — | — | deny | — | — | — | deny | — |
| `ast-editor_replace_function_body: (default)` | — | — | — | deny | — | — | — | deny | — |
| `ast-editor_replace_in_body: (default)` | — | — | — | deny | — | — | — | deny | — |
| `bash: (default)` | — | — | allow | — | — | — | — | — | — |
| `bash: *` | allow | allow | — | allow | allow | allow | allow | deny | allow |
| `bash: *basedpyright*` | allow | allow | — | allow | — | — | — | allow | allow |
| `bash: *coverage*` | — | — | — | — | — | — | — | allow | — |
| `bash: *docker *` | — | — | — | allow | — | — | — | — | — |
| `bash: *git*` | — | — | — | — | — | — | — | ask | — |
| `bash: *git*checkout *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: *git*reset *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: *mypy*` | allow | allow | — | allow | — | — | — | allow | allow |
| `bash: *nox*` | — | — | — | — | — | — | — | allow | — |
| `bash: *pytest*` | allow | allow | — | allow | — | — | — | allow | allow |
| `bash: *ruff*` | allow | allow | — | allow | — | — | — | allow | allow |
| `bash: *tox*` | — | — | — | — | — | — | — | allow | — |
| `bash: Get-ChildItem*` | allow | allow | — | — | allow | allow | — | — | allow |
| `bash: Remove-Item -Force *` | — | — | — | ask | — | — | — | ask | — |
| `bash: Remove-Item -Force*` | deny | deny | — | — | deny | deny | deny | — | deny |
| `bash: Remove-Item -Recurse -Force *` | — | — | — | ask | — | — | — | ask | — |
| `bash: Remove-Item -Recurse -Force*` | deny | deny | — | — | deny | deny | deny | — | deny |
| `bash: Set-ExecutionPolicy*` | deny | deny | — | deny | deny | deny | deny | — | deny |
| `bash: alembic*` | — | — | — | allow | — | — | — | — | — |
| `bash: apt purge *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: apt remove *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: brew uninstall *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: chmod -R 000 *` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: chmod -R 777 *` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: chown -R *` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: crontab -e*` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: crontab -r*` | deny | deny | — | deny | deny | deny | deny | — | deny |
| `bash: curl -X DELETE*` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: curl -X POST*` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: curl -X PUT*` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: curl*` | allow | allow | — | allow | allow | allow | — | allow | allow |
| `bash: dd if=* of=*` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: diskpart*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: docker --version` | — | — | — | — | allow | allow | — | — | — |
| `bash: docker build*` | — | — | — | allow | — | — | — | — | — |
| `bash: docker compose` | allow | allow | — | — | allow | allow | — | — | allow |
| `bash: docker compose *` | — | — | — | allow | — | — | — | — | — |
| `bash: docker compose build*` | — | — | — | allow | — | — | — | allow | — |
| `bash: docker compose config*` | allow | allow | — | allow | allow | allow | — | allow | allow |
| `bash: docker compose down*` | allow | allow | — | allow | ask | allow | allow | allow | allow |
| `bash: docker compose exec*` | — | — | — | allow | — | — | — | allow | — |
| `bash: docker compose logs*` | allow | allow | — | allow | allow | allow | — | allow | allow |
| `bash: docker compose ps*` | allow | allow | — | allow | allow | allow | — | allow | allow |
| `bash: docker compose restart*` | — | — | — | allow | — | — | — | allow | — |
| `bash: docker compose run*` | — | — | — | allow | — | — | — | allow | — |
| `bash: docker compose up*` | — | — | — | allow | — | — | — | allow | — |
| `bash: docker container prune*` | allow | allow | — | allow | ask | allow | allow | allow | allow |
| `bash: docker exec*` | — | — | — | allow | — | — | — | allow | — |
| `bash: docker image prune -a*` | allow | allow | — | allow | ask | allow | allow | allow | allow |
| `bash: docker inspect*` | allow | allow | — | allow | allow | allow | — | allow | allow |
| `bash: docker logs*` | allow | allow | — | allow | allow | allow | — | allow | allow |
| `bash: docker network prune*` | allow | allow | — | allow | ask | allow | allow | allow | allow |
| `bash: docker network*` | allow | allow | — | allow | — | — | — | — | allow |
| `bash: docker ps*` | allow | allow | — | allow | allow | allow | — | allow | allow |
| `bash: docker rm -f*` | allow | allow | — | allow | ask | allow | allow | allow | allow |
| `bash: docker rm*` | allow | allow | — | allow | — | allow | allow | allow | allow |
| `bash: docker rmi -f*` | allow | allow | — | allow | ask | allow | allow | allow | allow |
| `bash: docker run*` | — | — | — | allow | — | — | — | allow | — |
| `bash: docker system prune --volumes -a*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: docker system prune -a*` | allow | allow | — | allow | ask | allow | allow | allow | allow |
| `bash: docker system*` | allow | allow | — | allow | — | — | — | — | allow |
| `bash: docker volume prune*` | allow | allow | — | allow | ask | allow | allow | allow | allow |
| `bash: docker volume rm*` | allow | allow | — | allow | ask | allow | allow | allow | allow |
| `bash: docker volume*` | allow | allow | — | allow | — | — | — | — | allow |
| `bash: fdisk*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: format*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: gc --prune=now*` | — | — | — | ask | — | — | — | — | — |
| `bash: git *` | — | — | — | — | — | — | ask | — | — |
| `bash: git --version` | — | — | — | — | allow | allow | — | — | — |
| `bash: git add*` | — | — | — | — | — | — | — | deny | — |
| `bash: git branch -D*` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: git branch -d*` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: git branch*` | — | — | — | — | — | — | — | deny | — |
| `bash: git checkout*` | — | — | — | — | — | — | — | deny | — |
| `bash: git cherry-pick *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: git cherry-pick*` | — | — | — | — | — | — | — | deny | — |
| `bash: git clean *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: git clean -fd*` | deny | deny | — | — | deny | deny | deny | deny | deny |
| `bash: git clean -fdx*` | deny | deny | — | — | deny | deny | deny | deny | deny |
| `bash: git commit --amend*` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: git commit*` | — | — | — | — | — | — | — | deny | — |
| `bash: git diff*` | — | — | — | — | — | — | — | allow | — |
| `bash: git filter-branch*` | deny | deny | — | — | deny | deny | deny | deny | deny |
| `bash: git filter-repo*` | deny | deny | — | — | deny | deny | deny | deny | deny |
| `bash: git gc --prune=now*` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: git log *` | allow | allow | — | — | — | — | — | — | allow |
| `bash: git log*` | — | — | — | — | — | — | — | allow | — |
| `bash: git merge*` | — | — | — | — | — | — | — | deny | — |
| `bash: git push *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: git push --force*` | deny | deny | — | — | deny | deny | deny | deny | deny |
| `bash: git push --force-with-lease*` | deny | deny | — | — | deny | deny | deny | deny | deny |
| `bash: git push*` | — | — | — | — | — | — | — | deny | — |
| `bash: git rebase *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: git rebase*` | — | — | — | — | — | — | — | deny | — |
| `bash: git reflog expire*` | deny | deny | — | — | deny | deny | deny | — | deny |
| `bash: git reflog*` | — | — | — | — | — | — | — | deny | — |
| `bash: git reset --hard*` | deny | deny | — | — | deny | deny | deny | deny | deny |
| `bash: git reset*` | — | — | — | — | — | — | — | deny | — |
| `bash: git restore*` | — | — | — | — | — | — | — | deny | — |
| `bash: git show *` | allow | allow | — | — | — | — | — | — | allow |
| `bash: git show*` | — | — | — | — | — | — | — | allow | — |
| `bash: git stash *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: git stash*` | — | — | — | — | — | — | — | deny | — |
| `bash: git status*` | — | — | — | — | — | — | — | allow | — |
| `bash: git tag -d*` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: git update-ref -d*` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: git*add*` | — | — | — | allow | — | — | — | — | — |
| `bash: git*branch*` | — | — | — | ask | — | — | — | — | — |
| `bash: git*checkout *` | — | — | — | ask | — | — | — | — | — |
| `bash: git*cherry-pick *` | — | — | — | ask | — | — | — | — | — |
| `bash: git*clean *` | — | — | — | ask | — | — | — | — | — |
| `bash: git*clean*-fd*` | — | — | — | deny | — | — | — | — | — |
| `bash: git*clean*-fdx*` | — | — | — | deny | — | — | — | — | — |
| `bash: git*commit*` | — | — | — | allow | — | — | — | — | — |
| `bash: git*commit*--amend*` | — | — | — | ask | — | — | — | — | — |
| `bash: git*diff*` | — | — | — | allow | — | — | — | — | — |
| `bash: git*filter-branch*` | — | — | — | deny | — | — | — | — | — |
| `bash: git*filter-repo*` | — | — | — | deny | — | — | — | — | — |
| `bash: git*log*` | — | — | — | allow | — | — | — | — | — |
| `bash: git*ls-files*` | — | — | — | allow | — | — | — | — | — |
| `bash: git*merge*` | — | — | — | ask | — | — | — | — | — |
| `bash: git*push *` | — | — | — | ask | — | — | — | — | — |
| `bash: git*push*--force*` | — | — | — | deny | — | — | — | — | — |
| `bash: git*push*--force-with-lease*` | — | — | — | deny | — | — | — | — | — |
| `bash: git*rebase *` | — | — | — | ask | — | — | — | — | — |
| `bash: git*reflog*` | — | — | — | deny | — | — | — | — | — |
| `bash: git*reset *` | — | — | — | ask | — | — | — | — | — |
| `bash: git*reset*--hard*` | — | — | — | deny | — | — | — | — | — |
| `bash: git*restore*` | — | — | — | ask | — | — | ask | — | — |
| `bash: git*show*` | — | — | — | allow | — | — | — | — | — |
| `bash: git*stash*list*` | — | — | — | allow | — | — | — | — | — |
| `bash: git*stash*show*` | — | — | — | allow | — | — | — | — | — |
| `bash: git*status*` | — | — | — | allow | — | — | — | — | — |
| `bash: halt*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: iptables*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: kill -9 *` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: killall *` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: kubectl apply --force*` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: kubectl cordon *` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: kubectl delete *` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: kubectl delete deployment*` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: kubectl delete namespace*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: kubectl delete pod*` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: kubectl delete pv*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: kubectl delete pvc*` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: kubectl delete service*` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: kubectl describe*` | allow | allow | — | ask | allow | allow | — | ask | allow |
| `bash: kubectl drain *` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: kubectl exec*` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: kubectl get*` | allow | allow | — | allow | allow | allow | — | allow | allow |
| `bash: kubectl logs*` | allow | allow | — | allow | allow | allow | — | allow | allow |
| `bash: kubectl rollout undo*` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: kubectl top*` | allow | allow | — | allow | — | — | — | allow | allow |
| `bash: mkfs*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: mount *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: mv * /dev/null` | deny | deny | — | deny | deny | deny | deny | — | deny |
| `bash: node --version` | — | — | — | — | allow | allow | — | — | — |
| `bash: npm --version` | — | — | — | — | allow | allow | — | — | — |
| `bash: npm run build` | — | — | — | allow | — | — | — | allow | — |
| `bash: npm run lint*` | — | — | — | allow | — | — | — | allow | — |
| `bash: npm run typecheck*` | — | — | — | allow | — | — | — | allow | — |
| `bash: npm test*` | — | — | — | allow | — | — | — | allow | — |
| `bash: npm uninstall *` | ask | ask | — | — | ask | ask | ask | ask | ask |
| `bash: npm*install *` | — | — | — | ask | — | — | — | — | — |
| `bash: npm*uninstall *` | — | — | — | ask | — | — | — | — | — |
| `bash: parted*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: pip install *` | ask | ask | — | — | — | — | — | — | ask |
| `bash: pip uninstall *` | ask | ask | — | — | ask | ask | ask | ask | ask |
| `bash: pip*` | — | — | — | ask | — | — | — | — | — |
| `bash: pip*install *` | — | — | — | ask | — | — | — | — | — |
| `bash: pip*uninstall *` | — | — | — | ask | — | — | — | — | — |
| `bash: pkill *` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: poweroff*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: psql -c "ALTER *"` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: psql -c "DELETE FROM *"` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: psql -c "DROP *"` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: psql -c "GRANT *"` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: psql -c "REVOKE *"` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: psql -c "SELECT*"` | allow | allow | — | — | — | — | — | — | allow |
| `bash: psql -c "SHOW*"` | allow | allow | — | — | — | — | — | — | allow |
| `bash: psql -c "TRUNCATE *"` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: psql*` | — | — | — | ask | — | — | — | allow | — |
| `bash: python --version` | — | — | — | — | allow | allow | — | — | — |
| `bash: reboot*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: redis-cli DEL *` | — | — | — | ask | ask | ask | ask | ask | — |
| `bash: redis-cli FLUSHALL*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: redis-cli FLUSHDB*` | — | — | — | ask | ask | ask | ask | ask | — |
| `bash: redis-cli GET*` | allow | allow | — | — | — | — | — | — | allow |
| `bash: redis-cli KEYS*` | allow | allow | — | — | — | — | — | — | allow |
| `bash: redis-cli*` | — | — | — | allow | — | — | — | allow | — |
| `bash: reg add*` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: reg delete*` | deny | deny | — | deny | deny | deny | deny | — | deny |
| `bash: rm -r *` | — | — | — | ask | — | — | — | ask | — |
| `bash: rm -r*` | deny | deny | — | — | deny | deny | deny | — | deny |
| `bash: rm -rf *` | — | — | — | ask | — | — | — | ask | — |
| `bash: rm -rf*` | deny | deny | — | — | deny | deny | deny | — | deny |
| `bash: service * stop` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: setx *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: shred *` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: shutdown*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: systemctl disable *` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: systemctl stop *` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: truncate -s 0 *` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: ufw*` | deny | deny | — | deny | deny | deny | deny | deny | deny |
| `bash: umount *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `bash: uv *` | allow | allow | — | allow | — | — | — | allow | allow |
| `bash: uv --version` | — | — | — | — | allow | allow | — | — | — |
| `bash: uv pip install *` | — | — | — | ask | — | — | — | — | — |
| `bash: uv pip uninstall *` | ask | ask | — | ask | ask | ask | ask | ask | ask |
| `bash: uv run *` | — | — | — | allow | — | — | — | allow | — |
| `bash: uv run*` | allow | allow | — | — | — | — | — | — | allow |
| `bash: wipe *` | ask | ask | — | ask | ask | ask | ask | — | ask |
| `bash: yum remove *` | ask | ask | — | — | ask | ask | ask | — | ask |
| `edit: (default)` | — | — | — | — | — | — | — | deny | — |
| `edit: *` | deny | deny | deny | allow | deny | deny | allow | — | deny |
| `edit: *.adoc` | — | — | allow | — | — | — | — | — | — |
| `edit: *.ai\*` | allow | allow | — | allow | allow | allow | allow | — | allow |
| `edit: *.env` | — | — | — | allow | — | — | — | — | — |
| `edit: *.env.*` | — | — | — | allow | — | — | — | — | — |
| `edit: *.json` | — | — | — | — | — | allow | — | — | — |
| `edit: *.kilo\*` | allow | allow | — | allow | allow | allow | allow | — | allow |
| `edit: *.md` | allow | allow | allow | — | allow | allow | allow | — | allow |
| `edit: *.mdx` | — | — | allow | — | allow | allow | allow | — | — |
| `edit: *.rst` | — | — | allow | — | — | — | — | — | — |
| `edit: *.txt` | — | — | allow | — | — | — | — | — | — |
| `edit: *.yaml` | allow | allow | — | — | allow | allow | allow | — | allow |
| `edit: *.yml` | allow | allow | — | — | allow | allow | allow | — | allow |
| `edit: */CHANGELOG` | — | — | allow | — | — | — | — | — | — |
| `edit: */README` | — | — | allow | — | — | — | — | — | — |
| `edit: CHANGELOG` | — | — | allow | — | — | — | — | — | — |
| `edit: README` | — | — | allow | — | — | — | — | — | — |
| `glob: (default)` | allow | allow | — | allow | allow | allow | allow | allow | allow |
| `grep: (default)` | allow | allow | — | allow | allow | allow | allow | allow | allow |
| `mcp: (default)` | — | — | deny | — | — | — | — | — | — |
| `morfx_*: (default)` | — | — | — | deny | — | — | — | deny | — |
| `question: (default)` | — | — | allow | — | — | — | — | — | — |
| `read: (default)` | — | — | allow | — | — | — | — | — | — |
| `read: *` | allow | allow | — | allow | allow | allow | allow | allow | allow |
| `read: *.ai\*` | allow | allow | — | allow | allow | allow | allow | allow | allow |
| `read: *.env` | allow | allow | — | allow | allow | allow | allow | allow | allow |
| `read: *.env.*` | allow | allow | — | allow | allow | allow | allow | allow | allow |
| `read: *.kilo\*` | allow | allow | — | allow | allow | allow | allow | allow | allow |
| `task: (default)` | allow | — | — | allow | allow | allow | allow | deny | — |
| `todoread: (default)` | allow | allow | — | allow | allow | allow | allow | allow | allow |
| `todowrite: (default)` | — | — | — | allow | allow | allow | — | allow | — |
| `webfetch: (default)` | allow | allow | — | allow | — | allow | — | allow | allow |
| `websearch: (default)` | allow | allow | — | allow | — | allow | — | allow | allow |
