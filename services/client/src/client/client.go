package client

import (
	"bufio"
	"bytes"
	"net"
	"os"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet_serializer"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200


type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
	protocol *protocol.Protocol
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
	logger.Info("send-batch", logger.InProgress, messageArgs...)
	if err := client.protocol.SendMessage(batchPayload); err != nil {
		logger.Error("send-batch", logger.Fail, messageArgs...)
		return err
	}

	return nil
}

func (client *Client) sendBets(inputFile *os.File) error {
	scanner := bufio.NewScanner(inputFile)
	messageId := 0
	payloads := make([][]byte, 0, client.config.BatchSize)

	for scanner.Scan() {
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
				return err
			}
			messageId++
			payloads = payloads[:0]		
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-inputfile", logger.Fail, "path", inputFile)
		return err
	}

	// Send missing bets
	if err := client.sendBatch(payloads, messageId); err != nil {
		return err
	}

	if err := client.protocol.SendFin(); err != nil {
		return err
	}

	return nil
}

func (client *Client) recvResults(writer *bufio.Writer) error {
	for {
		responseBuffer, err := client.protocol.RecvMessage()
		if err != nil {
			logger.Error("recv-response", logger.Fail)
			return err
		}

		if protocol.IsFin(responseBuffer) {
			return nil
		}

		parsedBet, err := betserializer.DeserializeBet(responseBuffer)
		if err != nil {
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

	inputPath := os.Getenv("INPUT_FILE")
	inputFile, err := os.Open(inputPath)
	if err != nil {
		logger.Error("open-inputfile", logger.Fail, "path", inputPath)
		return err
	}
	defer inputFile.Close()

	outputPath := os.Getenv("OUTPUT_FILE")
	outputFile, err := os.Create(os.Getenv("OUTPUT_FILE"))
	if err != nil {
		logger.Error("open-outputfile", logger.Fail, "path", outputPath)
		return err
	}
	defer outputFile.Close()

	if err := client.sendBets(inputFile); err != nil {
		return err
	}

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()
	if err := client.recvResults(writer); err != nil {
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}
