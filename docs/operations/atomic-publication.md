# Atomic Publication Operations

M4 supports two explicit publication modes.

## Direct versus Atomic

Direct mode is the default when a Mirror has no `[publication]` table. The sync
process writes directly into `mirror_root/<target>`. Files become visible as the
process updates them. Direct mode preserves M3 behavior and works with M3 Agents
during the supported Server-first rolling upgrade.

Atomic mode is selected explicitly:

~~~toml
[publication]
mode = "atomic"
~~~

The Agent builds a fresh private candidate and changes the published directory
through one Linux namespace operation. New pathname resolution sees either the
old complete local tree or the new complete local tree. Attempt success is not
reported until the published-parent namespace has been fsynced and terminal
spool evidence is durable.

Atomic mode does not guarantee that the upstream was a point-in-time snapshot,
multi-request client consistency, recursive power-loss durability of every file,
or immediate invalidation of open files and serving caches.

## Storage configuration

Atomic-capable Agents require all three settings:

~~~toml
[storage]
mirror_root = "/srv/mirrors"
spool_dir = "/var/lib/lmt-agent/spool"
publication_root = "/srv/lmt-publication"
publication_max_private_generations = 4
publication_reserve_bytes = 10737418240
~~~

Choose values explicitly. `publication_root` must be outside `mirror_root`, but
both roots must reside on the same mounted local filesystem. The Agent execution
user must be able to write both roots. M4 probes `RENAME_EXCHANGE`,
`RENAME_NOREPLACE`, directory fsync, target type, and mount boundaries before it
advertises `atomic_exchange_v1`.

Run the offline local diagnostic while the Agent service is stopped:

~~~text
sudo -u lmt-agent lmt-agent --config /etc/lmt/agent.toml doctor
~~~

## Visibility, durability, and serving

The visibility linearization point is the directory exchange, or the
no-overwrite rename used for first publication. Already-open file descriptors
may continue reading old inodes. Nginx open-file caches may extend old-version
visibility, so cache configuration must be evaluated separately; LMT does not
reload Nginx or enter the download path.

The fixed private `exchange/` directory contains the immediately previous
generation after a completed update. It is retained for serving-reference grace
and diagnosis. It is not an isolated snapshot and M4 has no automatic rollback
API.

Atomic rsync may hard-link unchanged files between generations. Published and
previous generations are therefore immutable from LMT's perspective. Do not
modify content, modes, ownership, ACLs, or xattrs in place; such changes can
affect several generations through a shared inode. The serving stack must treat
the tree as content-read-only.

## Atomic rsync profile

Atomic rsync materializes a fresh generation. Files that exist only in the old
published tree do not carry forward. LMT owns `--link-dest`; every configured
option must be classified below. Unknown options are rejected.

Supported preservation and traversal:

~~~text
-a --archive  -r --recursive  -l --links  -p --perms  -t --times
-g --group  -o --owner  -D  -H --hard-links  -A --acls  -X --xattrs
--numeric-ids
~~~

Supported source selection, with fresh-generation meaning:

~~~text
--include  --exclude  --filter  --include-from  --exclude-from
--files-from  --prune-empty-dirs  --max-size  --min-size
~~~

Supported transport, performance, and comparison:

~~~text
--bwlimit  --timeout  --contimeout  -z --compress  --whole-file
--checksum  --size-only  --ignore-times  --block-size
--checksum-choice  --compress-choice  -s --protect-args
~~~

Supported observability:

~~~text
--itemize-changes  --stats  --human-readable  -v --verbose
-q --quiet  --progress
~~~

Supported source-link interpretation:

~~~text
--copy-links  --safe-links  --copy-unsafe-links
~~~

Safe options that may reduce hard-link deduplication efficiency include
`--checksum`, `--ignore-times`, `--whole-file`, and attribute combinations that
prevent a link-dest match.

Options whose destination-history meaning does not apply to a fresh generation
are rejected:

~~~text
--delete  --delete-before  --delete-during  --delete-delay  --delete-after
--delete-excluded  --max-delete  --force  --ignore-errors  --existing
--ignore-existing  --ignore-non-existing  --update
~~~

Unsafe or LMT-owned destination behavior is rejected:

~~~text
--inplace  --append  --append-verify  --write-devices
--link-dest  --copy-dest  --compare-dest  --backup  --backup-dir  --suffix
--partial  --partial-dir  --remove-source-files  --remove-sent-files
-n --dry-run  --list-only
~~~

LMT never silently strips an option. Use Direct mode when a repository requires
existing-destination semantics, or use a trusted custom command that explicitly
materializes the complete candidate.

## Recovery and fences

Protected phases (`preparing_exchange`, `ready_to_commit`,
`pre_visibility_recovery`, `visible_pending_durability`,
`committed_pending_report`, and `abandoned_fenced`) must not be deleted or reset.
Inspect exact local evidence with:

~~~text
lmt-agent publication status --mirror MIRROR
~~~

For a post-visibility durability failure, repair storage and use
`publication retry-durability` with the exact Mirror, Run, Attempt, and spec
hash. If durability can never be completed, the explicit risk-acknowledged
`publication abandon` operation records a durable full-writer fence before it
reports failure. It never rolls the published tree back.

`publication fence-clear` is separate and succeeds only after terminal/log
acknowledgement and stable local namespace checks. Until then the fence blocks
all LMT writers for that Mirror on the Node, including Direct mode.

Never repair publication state by manually exchanging directories, deleting
spool JSON, or removing referenced private generations.
