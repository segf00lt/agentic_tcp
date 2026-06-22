package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/jpoz/groq"
)

const (
	MESSAGE_FLAG_ACK        = 1 << iota
	MESSAGE_FLAG_BEGIN_TEXT = 1 << iota
	MESSAGE_FLAG_END_TEXT   = 1 << iota
)

const MESSAGE_DATA_SIZE = 2

type Message struct {
	Flags   uint32
	Seq_num uint32
	Data    [MESSAGE_DATA_SIZE]byte
}

type Metrics struct {
	Avg_rtt                float64 `json:"avg_rtt_micro_seconds"`
	Rtt_variance           float64 `json:"rtt_variance_micro_seconds"`
	RTO                    float64 `json:"rto_micro_seconds"`
	Loss_rate              float64 `json:"loss_rate"`
	Retransmission_rate    float64 `json:"retransmission_rate"`
	Throughput_bps         float64 `json:"throughput_bps"`
	Window_size            int     `json:"window_size"`
	Window_increase_amount int     `json:"window_increase_amount"`
	Window_decrease_factor float64 `json:"window_decrease_factor"`
}

type Decision struct {
	New_window_increase_amount int     `json:"new_window_increase_amount"`
	New_window_decrease_factor float64 `json:"new_window_decrease_factor"`
}

func send_text_message(text string, send_message_channel chan<- Message) {
	var message_backing_buf [128]Message

	n_messages_to_send := int(math.Ceil(float64(len(text)) / float64(MESSAGE_DATA_SIZE)))

	n_times := 1
	if n_messages_to_send > len(message_backing_buf) {
		n_times = int(math.Ceil(float64(n_messages_to_send) / float64(len(message_backing_buf))))
	}

	text_pos := 0
	for ; n_times > 0; n_times-- {
		messages_to_send := message_backing_buf[:0]

		for range n_messages_to_send {

			message := Message{}

			for j := 0; j < MESSAGE_DATA_SIZE && text_pos < len(text); j++ {
				message.Data[j] = text[text_pos]
				text_pos++
			}

			messages_to_send = append(messages_to_send, message)
		}

		messages_to_send[0].Flags |= MESSAGE_FLAG_BEGIN_TEXT
		messages_to_send[n_messages_to_send-1].Flags |= MESSAGE_FLAG_END_TEXT

		for _, m := range messages_to_send {
			send_message_channel <- m
		}
	}

}

func get_user_input(send_message_channel chan<- Message) {
	input_scanner := bufio.NewScanner(os.Stdin)
	defer input_scanner.Err()

	fmt.Print("> ")

	for input_scanner.Scan() {
		raw := input_scanner.Text()
		text := strings.TrimRight(raw, "\r\n")

		if len(text) == 0 {
			fmt.Print("> ")
			continue
		}

		send_text_message(text, send_message_channel)

		fmt.Print("> ")

	}
}

func get_test_input(send_message_channel chan<- Message) {

	rand_source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(rand_source)

	for {
		random_text := strings.Repeat("macaco", 1+rng.Intn(19))
		log.Printf("sending test message '%s'\n", random_text)

		send_text_message(random_text, send_message_channel)

		random_number_of_seconds := 5 + rng.Intn(12)
		time.Sleep(time.Second * time.Duration(random_number_of_seconds))
	}

}

func get_incoming_messages(conn *net.UDPConn, receive_message_channel chan<- Message) {
	buf := make([]byte, math.MaxUint16)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		var message Message
		if err := binary.Read(bytes.NewReader(buf[:n]), binary.BigEndian, &message); err != nil {
			continue
		}

		receive_message_channel <- message
	}

}

var test_rng *rand.Rand

func send_message_over_udp(conn *net.UDPConn, send_addr *net.UDPAddr, message Message) error {
	var buf bytes.Buffer

	// NOTE jfd 17/06/26: randomly drop messages
	// if test_rng.Float64() <= 0.1 {
	// 	return nil
	// }

	err := binary.Write(&buf, binary.BigEndian, message)
	if err != nil {
		return err
	}

	_, err = conn.WriteToUDP(buf.Bytes(), send_addr)

	return err
}

func groq_loop(metrics_channel <-chan Metrics, decision_channel chan<- Decision) {

	// NOTE jfd 22/06/26: don't use the LLM if AIMD is enabled
	if os.Getenv("AIMD_ENABLED") == "1" {
		return
	}

	client := groq.NewClient()

	if client == nil {
		panic("failed to create groq client!!!\n")
	}

	system_prompt :=
		`You tune AIMD for throughput. Prefer slightly more aggressive growth unless loss/retransmissions are high. If loss_rate < 0.02 and rtt_variance is low, increase window_increase_amount a bit; if loss_rate > 0.05 or retransmission_rate > 0.10, decrease it. Keep window_decrease_factor near 0.5–0.8, more conservative under loss. Make small changes only. Return only JSON:
{"new_window_increase_amount":int,"new_window_decrease_factor":float}`

	for metrics := range metrics_channel {

		metrics_json, err := json.Marshal(metrics)
		if err != nil {
			// TODO: handle the errors
			continue
		}
		groq_messages := []groq.Message{
			{
				Role:    "system",
				Content: system_prompt,
			},
			{
				Role:    "user",
				Content: string(metrics_json),
			},
		}
		response, err := client.CreateChatCompletion(groq.CompletionCreateParams{
			Model:          "llama-3.1-8b-instant",
			Messages:       groq_messages,
			ResponseFormat: groq.ResponseFormat{Type: "json_object"},
		})
		if err != nil {
			fmt.Println(err)
		}

		if len(response.Choices) > 0 {
			content := response.Choices[0].Message.Content
			var decision Decision
			err := json.Unmarshal([]byte(content), &decision)
			if err != nil {
				continue
			}
			decision_channel <- decision
		}

	}

}

func exponential_moving_average(avg float64, sample float64, coeficient float64) float64 {
	result := (1.0-coeficient)*avg + coeficient*sample
	return result
}

func main() {

	var err error

	err = godotenv.Load()

	LLM_ENABLED := os.Getenv("LLM_ENABLED")
	TEST_MODE_ENABLED := os.Getenv("TEST_MODE_ENABLED")

	metrics_log_file, err := os.OpenFile(fmt.Sprintf("metrics%d.log", os.Getpid()), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer metrics_log_file.Close()

	log.SetOutput(metrics_log_file)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("======= BEGIN METRICS =======")

	rand_source := rand.NewSource(time.Now().UnixNano())
	test_rng = rand.New(rand_source)

	listen_addr_flag := flag.String("listen", ":9001", "-listen <address to listen on>")
	send_addr_flag := flag.String("send", "127.0.0.1:9002", "-send <address to send on>")
	flag.Parse()

	fmt.Printf("listening on %s, sending on %s\n", *listen_addr_flag, *send_addr_flag)

	listen_addr, err := net.ResolveUDPAddr("udp", *listen_addr_flag)
	send_addr, err := net.ResolveUDPAddr("udp", *send_addr_flag)

	conn, err := net.ListenUDP("udp", listen_addr)
	if err != nil {
		log.Fatal("listen failed:", err)
	}
	defer conn.Close()

	send_message_channel := make(chan Message, 128)
	receive_message_channel := make(chan Message, 128)

	metrics_channel := make(chan Metrics, 128)
	decision_channel := make(chan Decision, 128)

	test_mode_enabled := (TEST_MODE_ENABLED == "1")
	llm_enabled := (LLM_ENABLED == "1")

	if test_mode_enabled {
		go get_test_input(send_message_channel)
	} else {
		go get_user_input(send_message_channel)
	}

	go get_incoming_messages(conn, receive_message_channel)

	if llm_enabled {
		go groq_loop(metrics_channel, decision_channel)
	}

	send_message_queue := make([]Message, 0, 128)
	receive_message_queue := make([]Message, 0, 128) // NOTE jfd 17/06/26: this stores the messages that arrive so that we can join them together once we have a begin and end text message
	in_flight_messages := make([]Message, 0, 128)
	in_flight_message_sent_times := make(map[uint32]time.Time)

	// NOTE jfd 19/06/26: tracking metrics for agentic congestion control
	average_rtt := 0.0
	rtt_variance := 0.0
	original_transmission_count := 0
	retransmission_count := 0
	lost_messages := 0
	successfully_acked_messages := 0
	bytes_acked := 0
	last_time_bytes_acked_was_measured := time.Now()
	throughput_bps := 0.0
	metrics_ticker := time.NewTicker(time.Second * 10)

	min_window_size := 1
	window_size := 4
	window_increase_amount := 1
	window_decrease_factor := 0.5
	window_base_seq_num := 0
	next_seq_num := 0          // next sequence number to use when sending messages
	next_expected_seq_num := 0 // next sequence number expected by the receiver
	print_backing_buf := make([]byte, 1024)
	count_acks_received := 0

	var timer *time.Timer
	var timeout_channel <-chan time.Time
	timeout_duration := time.Millisecond * 400

	// NOTE jfd 17/06/26: this timer stuff in go is very confusing
	stop_timer := func() {
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		timer = nil
		timeout_channel = nil
	}

	start_or_reset_timer := func() {
		if timer == nil {
			timer = time.NewTimer(timeout_duration)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout_duration)
		}
		timeout_channel = timer.C
	}

	for {

		received_end_text_message := false

		select {

		case <-metrics_ticker.C:

			current_metrics_are_valid := (average_rtt > 0.0 &&
				successfully_acked_messages > 0 &&
				original_transmission_count > 0)

			previous_decision_received := (len(decision_channel) == 0)

			if current_metrics_are_valid && previous_decision_received {

				elapsed := time.Since(last_time_bytes_acked_was_measured)
				throughput_sample := float64(bytes_acked) / elapsed.Seconds()

				throughput_bps = exponential_moving_average(throughput_bps, throughput_sample, 0.125)
				bytes_acked = 0
				last_time_bytes_acked_was_measured = time.Now()

				metrics := Metrics{
					Avg_rtt:                average_rtt,
					Rtt_variance:           rtt_variance,
					Loss_rate:              float64(lost_messages) / float64(lost_messages+successfully_acked_messages),
					Retransmission_rate:    float64(retransmission_count) / float64(retransmission_count+original_transmission_count),
					Throughput_bps:         throughput_bps,
					Window_size:            window_size,
					Window_increase_amount: window_increase_amount,
					Window_decrease_factor: window_decrease_factor,
					RTO:                    (average_rtt + 4*rtt_variance),
				}

				lost_messages = 0
				successfully_acked_messages = 0
				retransmission_count = 0
				original_transmission_count = 0

				log.Printf("%+v\n", metrics)

				if llm_enabled {
					metrics_channel <- metrics
				}
			}

		case <-timeout_channel:
			if len(in_flight_messages) == 0 {
				stop_timer()
			} else {
				log.Println("TIMEOUT")

				tmp := make([]Message, len(in_flight_messages))
				copy(tmp, in_flight_messages)
				send_message_queue = append(tmp, send_message_queue...)

				for _, m := range in_flight_messages {
					delete(in_flight_message_sent_times, m.Seq_num)
				}

				// TODO: put this in a struct
				lost_messages += len(in_flight_messages)
				retransmission_count += len(in_flight_messages)

				in_flight_messages = in_flight_messages[:0]

				// NOTE jfd 18/06/26: AIMD
				window_size = max(min_window_size, int(float64(window_size)*window_decrease_factor))
				count_acks_received = 0
				log.Printf("window_size = %d, congestion!!!\n", window_size)

			}

		case decision := <-decision_channel:
			log.Printf("agent decision: %+v", decision)
			window_increase_amount = decision.New_window_increase_amount
			window_decrease_factor = decision.New_window_decrease_factor

		case message_to_send := <-send_message_channel:
			message_to_send.Seq_num = uint32(next_seq_num)
			next_seq_num++
			send_message_queue = append(send_message_queue, message_to_send)

			// TODO: put this in a struct
			original_transmission_count++

		case message_received := <-receive_message_channel:
			is_ack := (message_received.Flags&MESSAGE_FLAG_ACK != 0)

			if is_ack {

				ack_num := int(message_received.Seq_num)

				ack_is_within_the_window := (ack_num >= window_base_seq_num && ack_num <= next_seq_num)

				if ack_is_within_the_window {
					acknowleged_any_in_flight_messages := false

					for len(in_flight_messages) > 0 && in_flight_messages[0].Seq_num <= uint32(ack_num) {

						message_acked := in_flight_messages[0]
						bytes_acked += len(message_acked.Data) * 8

						sent_time, ok := in_flight_message_sent_times[message_acked.Seq_num]
						{

							if ok {
								sample_rtt := float64(time.Since(sent_time)) / float64(time.Microsecond)
								delete(in_flight_message_sent_times, message_acked.Seq_num)

								// NOTE jfd 19/06/26: init the average and variance
								if average_rtt == 0 {
									average_rtt = sample_rtt
									rtt_variance = average_rtt / 2
								} else {
									average_rtt = exponential_moving_average(float64(average_rtt), float64(sample_rtt), 0.125)

									diff := math.Abs(float64(sample_rtt - average_rtt))

									rtt_variance = exponential_moving_average(float64(rtt_variance), diff, 0.25)
								}
							}

							// TODO: put this in a struct
							successfully_acked_messages++
						}

						in_flight_messages = in_flight_messages[1:]
						window_base_seq_num++
						acknowleged_any_in_flight_messages = true
						count_acks_received++
					}

					if count_acks_received >= window_size {
						// NOTE jfd 18/06/26: AIMD
						window_size += window_increase_amount
						count_acks_received = 0
					}

					if acknowleged_any_in_flight_messages {

						log.Printf("window_size = %d, increasing...\n", window_size)

						if len(in_flight_messages) == 0 {
							stop_timer()
						} else {
							start_or_reset_timer()
						}
					}

				}
			} else {
				seq_num := int(message_received.Seq_num)

				if seq_num == next_expected_seq_num {
					if message_received.Flags&MESSAGE_FLAG_BEGIN_TEXT != 0 {
						receive_message_queue = receive_message_queue[:0]
					}

					receive_message_queue = append(receive_message_queue, message_received)
					next_expected_seq_num++

					if message_received.Flags&MESSAGE_FLAG_END_TEXT != 0 {
						received_end_text_message = true
					}
					ack_message := Message{
						Flags:   MESSAGE_FLAG_ACK,
						Seq_num: message_received.Seq_num,
					}
					send_message_over_udp(conn, send_addr, ack_message)
				} else {
					ack_message := Message{
						Flags:   MESSAGE_FLAG_ACK,
						Seq_num: uint32(next_expected_seq_num) - 1,
					}
					send_message_over_udp(conn, send_addr, ack_message)
				}

			}

		}

		// NOTE jfd 17/06/26: fill the window with messages
		if len(send_message_queue) > 0 {

			window_was_empty := (len(in_flight_messages) == 0)

			for len(send_message_queue) > 0 && len(in_flight_messages) < window_size {
				message := send_message_queue[0]
				send_message_queue = send_message_queue[1:]

				if err := send_message_over_udp(conn, send_addr, message); err != nil {
					fmt.Fprintln(os.Stderr, "error while sending message:", err)
					send_message_queue = append([]Message{message}, send_message_queue...)
					break
				}
				in_flight_message_sent_times[message.Seq_num] = time.Now()

				in_flight_messages = append(in_flight_messages, message)
			}

			if window_was_empty && len(in_flight_messages) > 0 {
				start_or_reset_timer()
			}

		}

		if received_end_text_message {
			text_buf := print_backing_buf[:0]
			for _, m := range receive_message_queue {
				text_buf = append(text_buf, m.Data[0])
				if m.Data[1] != 0 {
					text_buf = append(text_buf, m.Data[1])
				}
			}

			fmt.Printf("peer@%s: %s\n", send_addr, string(text_buf))
			fmt.Print("> ")

			receive_message_queue = receive_message_queue[:0]

		}

	}

}
