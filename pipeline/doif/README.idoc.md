## Experimental: Do If rules

This is experimental feature and represents an advanced version of `match_fields`.
The Do If rules are a tree of nodes. The tree is stored in the Do If Checker instance.
When Do If Checker's Match func is called it calls to the root Match func and then
the chain of Match func calls are performed across the whole tree.

### Node types
@do-if-node|description

### Field op node
@do-if-field-op-node

### Field operations
@do-if-field-op|description

### Logical op node
@do-if-logical-op-node

### Logical operations
@do-if-logical-op|description

### Length comparison op node
@do-if-len-cmp-op-node

### Timestamp comparison op node
@do-if-ts-cmp-op-node
