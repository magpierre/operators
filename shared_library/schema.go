package sharedlibrary

type Schema struct {
	Fields []Field
}

type Field struct {
	FieldName     string
	FieldPosition int
	FieldType     string
	RowStats      RowStat
}

// GetField returns the position of a field in the schema by its name.
// If the field is not found, it returns -1.
func (s *Schema) GetField(fieldname string) int {
	for _, v := range s.Fields {
		if v.FieldName == fieldname {
			return v.FieldPosition
		}
	}
	return -1
}

// GetType returns the type of a field in the schema by its name.
// If the field is not found, it returns nil.
func (s *Schema) GetType(fieldname string) *string {
	for _, v := range s.Fields {
		if v.FieldName == fieldname {
			return &v.FieldType
		}
	}
	return nil
}
