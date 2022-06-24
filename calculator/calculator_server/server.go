package main

import (
	"context"
	"fmt"
	"github.com/AJackTi/grpc-go-course/calculator/calculatorpb"
	"google.golang.org/grpc"
	"log"
	"net"
)

type server struct {
	calculatorpb.UnimplementedCalculatorServiceServer
}

func (s *server) Sum(ctx context.Context, req *calculatorpb.SumRequest) (*calculatorpb.SumResponse, error) {
	fmt.Printf("Received Sum RPC: %v\n", req)
	firstNumber := req.FirstNumber
	secondNumber := req.SecondNumber

	return &calculatorpb.SumResponse{
		SumResult: firstNumber + secondNumber,
	}, nil
}

func (s *server) PrimeNumberDecomposition(req *calculatorpb.PrimeNumberDecompositionRequest,
	stream calculatorpb.CalculatorService_PrimeNumberDecompositionServer) error {
	fmt.Printf("Received PrimeNumberDecomposition RPC: %v\n", req)

	number := req.GetNumber()
	var divisor int64 = 2

	for number > 1 {
		if number%divisor == 0 {
			stream.Send(&calculatorpb.PrimeNumberDecompositionResponse{PrimeFactor: divisor})
			number /= divisor
		} else {
			divisor++
			fmt.Printf("Divisor has increased to %v\n", divisor)
		}
	}

	return nil
}

func main() {
	fmt.Println("Calculator Server")

	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	calculatorpb.RegisterCalculatorServiceServer(s, &server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
