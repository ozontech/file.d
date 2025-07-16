package ctor

import (
	"errors"
	"fmt"

	"github.com/ozontech/file.d/pipeline/logic"
)

const (
	fieldNameOp       = "op"
	fieldNameOperands = "operands"
)

type Node = map[string]any

func Extract[T any](
	root Node,
	opToNonLogicalCtor func(string) func(string, Node) (T, error),
	simpleLogicCtor func(op logic.Op, operands []T) T,
) (T, error) {
	var (
		extract        func(Node) (T, error)
		extractLogical func(string, Node) (T, error)
	)

	extract = func(node Node) (T, error) {
		var def T

		opName, err := Get[string](node, fieldNameOp)
		if err != nil {
			return def, err
		}

		switch opName {
		case logic.AndTag, logic.OrTag, logic.NotTag:
			return extractLogical(opName, node)
		}

		if curCtor := opToNonLogicalCtor(opName); curCtor != nil {
			return curCtor(opName, node)
		}

		return def, fmt.Errorf("unknown op: %s", opName)
	}

	extractLogical = func(op string, node Node) (T, error) {
		var def T

		rawOperands, err := Get[[]any](node, fieldNameOperands)
		if err != nil {
			return def, err
		}

		operands := make([]T, 0)

		for _, rawOperand := range rawOperands {
			var operandNode Node
			operandNode, err = Must[Node](rawOperand)
			if err != nil {
				return def, fmt.Errorf("logical node operand type mismatch: %w", err)
			}

			var operand T
			operand, err = Extract(operandNode, opToNonLogicalCtor, simpleLogicCtor)
			if err != nil {
				return def, fmt.Errorf("extract operand for logical op %q: %w", op, err)
			}

			operands = append(operands, operand)
		}

		result, err := NewLogicalNode(op, operands, simpleLogicCtor)
		if err != nil {
			return def, fmt.Errorf("init logical node: %w", err)
		}

		return result, nil
	}

	return extract(root)
}

func NewLogicalNode[T any](
	op string,
	operands []T,
	simpleLogicCtor func(op logic.Op, operands []T) T,
) (T, error) {
	var def T

	if len(operands) == 0 {
		return def, errors.New("logical op must have at least one operand")
	}

	logicOp, err := logic.StringToOp(op)
	if err != nil {
		return def, err
	}

	if logicOp == logic.Not && len(operands) != 1 {
		return def, fmt.Errorf("logical not must have exactly one operand, got %d", len(operands))
	}

	return simpleLogicCtor(logicOp, operands), nil
}
