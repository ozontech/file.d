# Experimental: Do If rules (logs content matching rules)

This is experimental feature and represents an advanced version of `match_fields`.
The Do If rules are a tree of nodes. The tree is stored in the Do If Checker instance.
When Do If Checker's Match func is called it calls to the root Match func and then
the chain of Match func calls are performed across the whole tree.

## Node types
@do-if-node|description

## String op node
@do-if-string-op-node

## String operations
@do-if-string-op

## Logical op node
@do-if-logical-op-node

## Logical operations
@do-if-logical-op

## Length comparison op node
@do-if-len-cmp-op-node

## Timestamp comparison op node
@do-if-ts-cmp-op-node

## Check type op node
@do-if-check-type-op-node
