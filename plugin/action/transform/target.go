package transform

// Segment is a single resolved step in a runtime path.
// Exactly one mode is active per segment.
type Segment struct {
	Field string // .fieldname - active when IsIndex is false
	Idx   int    // [n]        - active when IsIndex is true; negative == from end
}

func FieldSeg(name string) Segment { return Segment{Field: name} }
func IndexSeg(idx int) Segment     { return Segment{Idx: idx} }

func (s Segment) IsField() bool { return s.Field != "" }
func (s Segment) IsIndex() bool { return s.Field == "" }

// Path is a fully-resolved runtime path produced by the interpreter
// after evaluating any dynamic index expressions inside PathExpr.
type Path struct {
	Root     PathRoot  // EventRoot (.field) or MetadataRoot (%field)
	Segments []Segment // empty == event root or metadata root
}

// Abstraction over the event being processed.
//
// The interpreter accesses all data exclusively through this interface.
type Target interface {
	// Get retrieves the value at path.
	Get(path Path) (Value, error)

	// Set writes value at path, creating missing nodes as needed.
	Set(path Path, value Value) error

	// Delete removes the node at path. No-op when the path does not exist.
	Delete(path Path) error
}
