// Package store contains a datastore for authorization policy evaluation.
package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-jose/go-jose/v3"
	"github.com/open-policy-agent/opa/ast"
	"github.com/open-policy-agent/opa/rego"
	"github.com/open-policy-agent/opa/storage"
	"github.com/open-policy-agent/opa/storage/inmem"
	"github.com/open-policy-agent/opa/types"
	"google.golang.org/protobuf/proto"

	"github.com/pomerium/pomerium/config"
	"github.com/pomerium/pomerium/internal/log"
	"github.com/pomerium/pomerium/pkg/grpc/databroker"
)

// A Store stores data for the OPA rego policy evaluation.
type Store struct {
	storage.Store
}

// New creates a new Store.
func New() *Store {
	return &Store{
		Store: inmem.New(),
	}
}

// UpdateIssuer updates the issuer in the store. The issuer is used as part of JWT construction.
func (s *Store) UpdateIssuer(issuer string) {
	s.write("/issuer", issuer)
}

// UpdateGoogleCloudServerlessAuthenticationServiceAccount updates the google cloud serverless authentication
// service account in the store.
func (s *Store) UpdateGoogleCloudServerlessAuthenticationServiceAccount(serviceAccount string) {
	s.write("/google_cloud_serverless_authentication_service_account", serviceAccount)
}

// UpdateJWTClaimHeaders updates the jwt claim headers in the store.
func (s *Store) UpdateJWTClaimHeaders(jwtClaimHeaders map[string]string) {
	s.write("/jwt_claim_headers", jwtClaimHeaders)
}

// UpdateRoutePolicies updates the route policies in the store.
func (s *Store) UpdateRoutePolicies(routePolicies []config.Policy) {
	s.write("/route_policies", routePolicies)
}

// UpdateSigningKey updates the signing key stored in the database. Signing operations
// in rego use JWKs, so we take in that format.
func (s *Store) UpdateSigningKey(signingKey *jose.JSONWebKey) {
	s.write("/signing_key", signingKey)
}

func (s *Store) write(rawPath string, value interface{}) {
	ctx := context.TODO()
	err := storage.Txn(ctx, s.Store, storage.WriteParams, func(txn storage.Transaction) error {
		return s.writeTxn(txn, rawPath, value)
	})
	if err != nil {
		log.Error(ctx).Err(err).Msg("opa-store: error writing data")
		return
	}
}

func (s *Store) writeTxn(txn storage.Transaction, rawPath string, value interface{}) error {
	p, ok := storage.ParsePath(rawPath)
	if !ok {
		return fmt.Errorf("invalid path")
	}

	if len(p) > 1 {
		err := storage.MakeDir(context.Background(), s, txn, p[:len(p)-1])
		if err != nil {
			return err
		}
	}

	var op storage.PatchOp = storage.ReplaceOp
	_, err := s.Read(context.Background(), txn, p)
	if storage.IsNotFound(err) {
		op = storage.AddOp
	} else if err != nil {
		return err
	}

	return s.Write(context.Background(), txn, op, p, value)
}

// GetDataBrokerRecordOption returns a function option that can retrieve databroker data.
func (s *Store) GetDataBrokerRecordOption() func(*rego.Rego) {
	return rego.Function2(&rego.Function{
		Name: "get_databroker_record",
		Decl: types.NewFunction(
			types.Args(types.S, types.S),
			types.NewObject(nil, types.NewDynamicProperty(types.S, types.S)),
		),
	}, func(bctx rego.BuiltinContext, op1 *ast.Term, op2 *ast.Term) (*ast.Term, error) {
		recordType, ok := op1.Value.(ast.String)
		if !ok {
			return nil, fmt.Errorf("invalid record type: %T", op1)
		}

		value, ok := op2.Value.(ast.String)
		if !ok {
			return nil, fmt.Errorf("invalid record id: %T", op2)
		}

		getter := databroker.GetGetter(bctx.Context)
		res, err := getter.Get(bctx.Context, &databroker.GetRequest{
			Type: string(recordType),
			Id:   string(value),
		})
		if storage.IsNotFound(err) {
			return ast.NullTerm(), nil
		} else if err != nil {
			return nil, err
		}

		msg, _ := res.GetRecord().GetData().UnmarshalNew()
		if msg == nil {
			return ast.NullTerm(), nil
		}
		obj := toMap(msg)

		regoValue, err := ast.InterfaceToValue(obj)
		if err != nil {
			return nil, err
		}

		return ast.NewTerm(regoValue), nil
	})
}

func toMap(msg proto.Message) map[string]interface{} {
	bs, _ := json.Marshal(msg)
	var obj map[string]interface{}
	_ = json.Unmarshal(bs, &obj)
	return obj
}
