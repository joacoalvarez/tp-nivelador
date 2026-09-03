package client

import (
	"bufio"
	"net"
	"os"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const ECHO_CLIENT_BUFFER_SIZE = 512
const ECHO_CLIENT_MESSAGE_AMOUNT = 3
const ECHO_CLIENT_MESSAGE_DELAY_MS = 1000

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
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

func (client *Client) sendAndPersistResponse(recordMessage string, writer *bufio.Writer, messageId int) error {
    messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}
    logger.Info("test-echo-server", logger.InProgress, messageArgs...)

    if err := safe_socket.SendAll(client.conn, []byte(recordMessage)); err != nil {
        logger.Error("send-message", logger.Fail, messageArgs...)
        return err
    }

    responseBuffer, err := safe_socket.RecvAll(client.conn, ECHO_CLIENT_BUFFER_SIZE)
    if err != nil {
        logger.Error("recv-response", logger.Fail, messageArgs...)
        return err
    }

	_, err = writer.Write(responseBuffer)
	if err != nil {
		logger.Error("write-to-buffer", logger.Fail, messageArgs...)
		return err
	}

	if _, err = writer.WriteString("\n"); err != nil {
		logger.Error("write-newline", logger.Fail, messageArgs...)
		return err
	}

    return nil
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

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	scanner := bufio.NewScanner(inputFile)
	messageId := 0
	for scanner.Scan() {
		if err := client.sendAndPersistResponse(scanner.Text(), writer, messageId); err != nil {
			return err
		}

		messageId++
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-inputfile", logger.Fail, "path", inputPath)
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}
