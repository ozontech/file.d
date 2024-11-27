package decoder

import (
	"context"
	"fmt"
	"strings"

	"github.com/bufbuild/protocompile"
	insaneJSON "github.com/ozontech/insane-json"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	protoFileSuffix   = ".proto"
	protoInmemoryFile = "inmemory.proto"

	protoFileParam        = "proto_file"
	protoMessageParam     = "proto_message"
	protoImportPathsParam = "proto_import_paths"
)

type protobufParams struct {
	file        string   // required
	message     string   // required
	importPaths []string // optional
}

type protobufDecoder struct {
	msgDesc protoreflect.MessageDescriptor
}

func NewProtobufDecoder(params map[string]any) (Decoder, error) {
	p, err := extractProtobufParams(params)
	if err != nil {
		return nil, fmt.Errorf("can't extract params: %w", err)
	}

	resolver := &protocompile.SourceResolver{}
	if p.importPaths != nil {
		resolver.ImportPaths = p.importPaths
	}

	fileName := p.file
	if !strings.HasSuffix(p.file, protoFileSuffix) {
		resolver.Accessor = protocompile.SourceAccessorFromMap(map[string]string{
			protoInmemoryFile: p.file,
		})
		fileName = protoInmemoryFile
	}

	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(resolver),
	}

	files, err := compiler.Compile(context.Background(), fileName)
	if err != nil {
		return nil, fmt.Errorf("can't compile proto-file %q: %w", fileName, err)
	}

	f := files.FindFileByPath(fileName)
	if f == nil {
		return nil, fmt.Errorf("can't find proto-file %q after compilation", fileName)
	}

	msgDesc := f.Messages().ByName(protoreflect.Name(p.message))
	if msgDesc == nil {
		return nil, fmt.Errorf("can't find message %q in proto-file %q", p.message, fileName)
	}

	return &protobufDecoder{
		msgDesc: msgDesc,
	}, nil
}

func (d *protobufDecoder) Type() Type {
	return PROTOBUF
}

func (d *protobufDecoder) DecodeToJson(root *insaneJSON.Root, data []byte) error {
	msgJson, err := d.Decode(data)
	if err != nil {
		return err
	}
	_ = root.DecodeBytes(msgJson.([]byte))
	return nil
}

func (d *protobufDecoder) Decode(data []byte, _ ...any) (any, error) {
	msg := dynamicpb.NewMessage(d.msgDesc)
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal proto: %w", err)
	}

	msgJson, err := protojson.MarshalOptions{
		EmitDefaultValues: true,
	}.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal proto to json: %w", err)
	}

	return msgJson, nil
}

func extractProtobufParams(params map[string]any) (protobufParams, error) {
	fileRaw, ok := params[protoFileParam]
	if !ok {
		return protobufParams{}, fmt.Errorf("%q not set", protoFileParam)
	}
	file, ok := fileRaw.(string)
	if !ok {
		return protobufParams{}, fmt.Errorf("%q must be string", protoFileParam)
	}

	msgRaw, ok := params[protoMessageParam]
	if !ok {
		return protobufParams{}, fmt.Errorf("%q not set", protoMessageParam)
	}
	msg, ok := msgRaw.(string)
	if !ok {
		return protobufParams{}, fmt.Errorf("%q must be string", protoMessageParam)
	}

	var importPaths []string
	if importPathsRaw, ok := params[protoImportPathsParam]; ok {
		importPathsSlice, ok := importPathsRaw.([]any)
		if !ok {
			return protobufParams{}, fmt.Errorf("%q must be slice", protoImportPathsParam)
		}
		importPaths = make([]string, 0, len(importPathsSlice))
		for _, v := range importPathsSlice {
			vStr, ok := v.(string)
			if !ok {
				return protobufParams{}, fmt.Errorf("each element in %q must be string", protoImportPathsParam)
			}
			importPaths = append(importPaths, vStr)
		}
	}

	return protobufParams{
		file:        file,
		message:     msg,
		importPaths: importPaths,
	}, nil
}
