//go:generate protoc --grpc-gateway_out=. --grpc-gateway_opt paths=source_relative --grpc-gateway_opt generate_unbound_methods=true --go-grpc_out=. --go-grpc_opt=paths=source_relative --go_out=. --go_opt=paths=source_relative common/v1/common.proto runtime/v1/admin.proto scheduler/v1/resources.proto scheduler/v1/scheduler.proto
//go:generate protoc --grpc-gateway_out=. --grpc-gateway_opt=paths=source_relative --grpc-gateway_opt=generate_unbound_methods=true --go-grpc_out=. --go-grpc_opt=paths=source_relative --go_out=. --go_opt=paths=source_relative jobs/v1/jobs.proto
package runtime
