package client

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet_serializer"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200
const BATCH_RETRIES_MAX = 3

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	BatchSize  int
}

type Client struct {
	conn              net.Conn
	config            ClientConfig
	protocol          *protocol.Protocol
	shutdownRequested bool
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config, protocol: protocol.NewProtocol(conn)}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) sendBatch(payloads [][]byte, messageId int) error {
	if len(payloads) == 0 {
		return nil
	}

	batchPayload := bytes.Join(payloads, nil)
	messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}

	for attempt := 0; attempt < BATCH_RETRIES_MAX; attempt++ {
		if client.shutdownRequested {
			return nil
		}

		logger.Info("send-batch", logger.InProgress, append(messageArgs, "attempt", attempt)...)
		if err := client.protocol.SendDataMessage(batchPayload); err != nil {
			if client.shutdownRequested {
				return nil
			}
			logger.Error("send-batch", logger.Fail, append(messageArgs, "attempt", attempt)...)
			return err
		}

		opcode, _, err := client.protocol.RecvMessage()
		if err != nil {
			if client.shutdownRequested {
				return nil
			}
			logger.Error("recv-ack", logger.Fail, append(messageArgs, "attempt", attempt)...)
			return err
		}

		if opcode == protocol.OpcodeAck {
			return nil
		}

		if opcode == protocol.OpcodeErr {
			logger.Warn("recv-err-resending", logger.InProgress, append(messageArgs, "attempt", attempt)...)
			continue
		}

		logger.Error("unexpected-opcode", logger.Fail, "opcode", opcode)
		return fmt.Errorf("unexpected opcode received: %d", opcode)
	}

	logger.Error("send-batch-max-retries", logger.Fail, messageArgs...)
	return fmt.Errorf("exceeded maximum retries for batch sending after receiving ERR")
}

func (client *Client) sendBets(inputFile *os.File) error {
	scanner := bufio.NewScanner(inputFile)
	messageId := 0
	payloads := make([][]byte, 0, client.config.BatchSize)

	for scanner.Scan() {
		if client.shutdownRequested {
			return nil
		}
		agencyID := client.config.AgencyId
		parsedBet, err := betserializer.ParseBet(scanner.Text(), agencyID)
		if err != nil {
			return err
		}

		payload, err := betserializer.SerializeBet(parsedBet)
		if err != nil {
			return err
		}

		payloads = append(payloads, payload)
		if len(payloads) == client.config.BatchSize {
			if err := client.sendBatch(payloads, messageId); err != nil {
				if client.shutdownRequested {
					return nil
				}
				return err
			}
			messageId++
			payloads = payloads[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		if client.shutdownRequested {
			return nil
		}
		logger.Error("read-inputfile", logger.Fail, "path", inputFile)
		return err
	}

	if client.shutdownRequested {
		return nil
	}

	// Send missing bets
	if err := client.sendBatch(payloads, messageId); err != nil {
		if client.shutdownRequested {
			return nil
		}
		return err
	}

	if err := client.protocol.SendFin(); err != nil {
		if client.shutdownRequested {
			return nil
		}
		return err
	}

	return nil
}

func (client *Client) recvResults(writer *bufio.Writer) error {
	for {
		if client.shutdownRequested {
			return nil
		}

		opcode, responseBuffer, err := client.protocol.RecvMessage()
		if err != nil {
			if client.shutdownRequested {
				return nil
			}
			logger.Error("recv-response", logger.Fail)
			return err
		}

		if opcode == protocol.OpcodeFin {
			return nil
		}

		if opcode == protocol.OpcodeErr {
			logger.Error("recv-server-err", logger.Fail)
			return fmt.Errorf("server returned error response")
		}

		if opcode != protocol.OpcodeData {
			logger.Error("unexpected-opcode-recv-results", logger.Fail, "opcode", opcode)
			return fmt.Errorf("unexpected opcode in recvResults: %d", opcode)
		}

		parsedBet, err := betserializer.DeserializeBet(responseBuffer)
		if err != nil {
			if client.shutdownRequested {
				return nil
			}
			logger.Error("deserialize-response", logger.Fail)
			return err
		}

		if _, err = writer.Write(betserializer.BetToCSV(parsedBet)); err != nil {
			return err
		}

		if _, err = writer.WriteString("\n"); err != nil {
			return err
		}
	}
}

func (client *Client) Run() error {
	const mainAction = "test-echo-server"
	defer client.conn.Close()

	// Set up signal handling for graceful shutdown
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)
	client.shutdownRequested = false

	// Goroutine to listen for shutdown signals
	go func() {
		<-shutdownChan
		client.shutdownRequested = true
		logger.Info("graceful-shutdown", logger.InProgress)
		client.conn.Close()
	}()

	inputPath := os.Getenv("INPUT_FILE")
	inputFile, err := os.Open(inputPath)
	if err != nil {
		if client.shutdownRequested {
			logger.Info("graceful-shutdown", logger.Success)
			return nil
		}
		logger.Error("open-inputfile", logger.Fail, "path", inputPath)
		return err
	}
	defer inputFile.Close()

	outputPath := os.Getenv("OUTPUT_FILE")
	outputFile, err := os.Create(outputPath)
	if err != nil {
		if client.shutdownRequested {
			logger.Info("graceful-shutdown", logger.Success)
			return nil
		}
		logger.Error("open-outputfile", logger.Fail, "path", outputPath)
		return err
	}
	defer outputFile.Close()

	if err := client.sendBets(inputFile); err != nil {
		if client.shutdownRequested {
			logger.Info("graceful-shutdown", logger.Success)
			return nil
		}
		return err
	}

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()
	if err := client.recvResults(writer); err != nil {
		if client.shutdownRequested {
			logger.Info("graceful-shutdown", logger.Success)
			return nil
		}
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}
