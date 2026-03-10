# pgcompact
A poor man's pgrepack.

I like pgrepack, but it takes a long-lived lock to block schema changes. This interacts negatively with autovacuum in busy databases.

This tool does not take such a lock, and is only safe to use if you know there will not be concurrent schema changes.

This tool was written with Claude Code. YMMV, use at your own risk.
