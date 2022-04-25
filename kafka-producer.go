package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"

	"github.com/Shopify/sarama"
)

type timeElapsed struct {
	elapsedTime   time.Duration
	nSendCntTotal int
}

var (
	brokerList  = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster. You can also set the KAFKA_PEERS environment variable")
	topic       = flag.String("topic", "", "REQUIRED: the topic to produce to")
	key         = flag.String("key", "", "The key of the message to produce. Can be empty.")
	partitioner = flag.String("partitioner", "", "The partitioning scheme to use. Can be `hash`, `manual`, or `random`")
	partition   = flag.Int("partition", -1, "The partition to produce to.")
	showMetrics = flag.Bool("metrics", false, "Output metrics on successful publish to stderr")

	pstLogTime  = flag.String("logtime", "ztime.json", "logtime name")
	pnPackSize  = flag.Int("size", 512, "packet size")
	pnPackCount = flag.Int("count", 1000000, "packet count")

	startTime      time.Time
	wg             sync.WaitGroup
	srtTimeElapsed []timeElapsed
)

func main() {
	var (
		nElapsedCnt, nElapsedIx = 0, 0
		ix, nSendCntTotal       = 0, 0
	)

	flag.Parse()

	if *brokerList == "" {
		printUsageErrorAndExit("no -brokers specified. Alternatively, set the KAFKA_PEERS environment variable")
	}

	if *topic == "" {
		printUsageErrorAndExit("no -topic specified")
	}

	bsBufS := make([]byte, *pnPackSize)

	for ix := 0; ix < *pnPackSize; ix++ {
		bsBufS[ix] = 'a'
	}

	nElapsedCnt = *pnPackCount/1000 + 30
	srtTimeElapsed = make([]timeElapsed, nElapsedCnt)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	switch *partitioner {
	case "":
		if *partition >= 0 {
			config.Producer.Partitioner = sarama.NewManualPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewHashPartitioner
		}
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "roundrobin":
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if *partition == -1 {
			printUsageErrorAndExit("-partition is required when partitioning manually")
		}
	default:
		printUsageErrorAndExit(fmt.Sprintf("Partitioner %s not supported.", *partitioner))
	}

	fpTime, _ := os.OpenFile(*pstLogTime, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	// 시작 시간
	startTime = time.Now()

	// 수신
	wg.Add(1)
	go func() {

		defer wg.Done()

		message := &sarama.ProducerMessage{Topic: *topic, Partition: int32(*partition)}

		if *key != "" {
			message.Key = sarama.StringEncoder(*key)
		}

		message.Value = sarama.ByteEncoder(bsBufS)

		producer, err := sarama.NewSyncProducer(strings.Split(*brokerList, ","), config)
		if err != nil {
			printErrorAndExit(69, "Failed to open Kafka producer: %s", err)
		}

		defer func() {
			if err := producer.Close(); err != nil {
				fmt.Println("Failed to close Kafka producer cleanly:", err)
			}
		}()

		for ix = 0; ix < *pnPackCount; ix++ {
			nSendCntTotal++

			partition, offset, err := producer.SendMessage(message)
			if err != nil {
				printErrorAndExit(69, "Failed to produce message: %s", err)
			}
			fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", *topic, partition, offset)

			// 경과 시간 저장
			if nSendCntTotal == 1 ||
				(nSendCntTotal >= 10 && nSendCntTotal < 100 && nSendCntTotal%10 == 0) ||
				(nSendCntTotal >= 100 && nSendCntTotal < 1000 && nSendCntTotal%100 == 0) ||
				(nSendCntTotal%1000 == 0) {
				if nElapsedIx < nElapsedCnt {
					srtTimeElapsed[nElapsedIx].nSendCntTotal = nSendCntTotal
					srtTimeElapsed[nElapsedIx].elapsedTime = time.Since(startTime)
					nElapsedIx++
				} else {
					fmt.Printf("[W] ElapsedIx(%d) < ELASPIX\n", nElapsedIx)
				}
			}
		}
	}()

	wg.Wait()

	if *showMetrics {
		metrics.WriteOnce(config.MetricRegistry, os.Stderr)
	}

	// 타임로그 작성
	if nElapsedIx < nElapsedCnt {
		srtTimeElapsed[nElapsedIx].nSendCntTotal = nSendCntTotal + 1
		srtTimeElapsed[nElapsedIx].elapsedTime = time.Since(startTime)
		nElapsedIx++
	}
	for ix = 0; ix < nElapsedCnt; ix++ {
		if srtTimeElapsed[ix].nSendCntTotal > 0 {
			fmt.Fprintf(fpTime, "{ \"index\" : { \"_index\" : \"commspeed\", \"_type\" : \"record\", \"_id\" : \"%v\" } }\n{\"sync\":\"async\", \"packsize\":%d, \"packcnt\":%d, \"escount\":%d, \"estime\":%v}\n",
				time.Now().UnixNano(), *pnPackSize, *pnPackCount, srtTimeElapsed[ix].nSendCntTotal, srtTimeElapsed[ix].elapsedTime)
		}
	}

	// 종료
	_ = fpTime.Close()
	fmt.Println("End")
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}
