package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	"github.com/alexrett/orchigram/internal/resource"
)

const resourceTokenVersion = 1

type resourcePageToken struct {
	Version      int    `json:"version"`
	Revision     uint64 `json:"revision"`
	FilterDigest string `json:"filterDigest"`
	Kind         string `json:"kind"`
	Namespace    string `json:"namespace"`
	Name         string `json:"name"`
}

type resourceFilter struct {
	Kind      string      `json:"kind"`
	Namespace string      `json:"namespace"`
	Labels    []labelPair `json:"labels"`
}

type labelPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func decodeResourcePageToken(request *controlv1alpha1.ListRequest) (resourcePageToken, error) {
	if request.GetContinueToken() == "" {
		return resourcePageToken{}, nil
	}
	if len(request.GetContinueToken()) > 8192 {
		return resourcePageToken{}, errors.New("continue_token exceeds size limit")
	}
	data, err := base64.RawURLEncoding.DecodeString(request.GetContinueToken())
	if err != nil {
		return resourcePageToken{}, errors.New("continue_token is invalid")
	}
	var token resourcePageToken
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&token); err != nil {
		return resourcePageToken{}, errors.New("continue_token is invalid")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return resourcePageToken{}, errors.New("continue_token is invalid")
	}
	if token.Version != resourceTokenVersion || token.Revision == 0 || !validResourceKind(token.Kind) || token.Namespace == "" || token.Name == "" {
		return resourcePageToken{}, errors.New("continue_token is invalid")
	}
	if token.FilterDigest != resourceFilterDigest(request) {
		return resourcePageToken{}, errors.New("continue_token does not match list filters")
	}
	if err := resource.ValidateMetadata(resource.ObjectMeta{Name: token.Name, Namespace: token.Namespace}); err != nil {
		return resourcePageToken{}, errors.New("continue_token is invalid")
	}
	return token, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing token data")
	}
	return nil
}

func validResourceKind(kind string) bool {
	switch kind {
	case "Flow", "Trigger", "Repository", "AgentProfile", "PluginInstallation", "SecretRef":
		return true
	default:
		return false
	}
}

func encodeResourcePageToken(request *controlv1alpha1.ListRequest, revision uint64, last resource.Document) (string, error) {
	token := resourcePageToken{
		Version: resourceTokenVersion, Revision: revision, FilterDigest: resourceFilterDigest(request),
		Kind: last.Kind, Namespace: last.Metadata.Namespace, Name: last.Metadata.Name,
	}
	data, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func resourceFilterDigest(request *controlv1alpha1.ListRequest) string {
	filter := resourceFilter{Kind: request.GetKind(), Namespace: request.GetNamespace()}
	keys := make([]string, 0, len(request.GetLabels()))
	for key := range request.GetLabels() {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		filter.Labels = append(filter.Labels, labelPair{Key: key, Value: request.GetLabels()[key]})
	}
	data, err := json.Marshal(filter)
	if err != nil {
		panic(fmt.Sprintf("marshal resource list filter: %v", err))
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
