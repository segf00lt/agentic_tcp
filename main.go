package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
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

// TODO jfd 18/06/26: Send metrics to agent
type Metrics struct {
	Avg_rtt             time.Duration
	Rtt_variance        time.Duration
	Loss_rate           float64
	Retransmission_rate float64
	Throughput_bps      float64
	Window_size         int
}

func get_user_input(send_message_channel chan<- Message) {
	input_scanner := bufio.NewScanner(os.Stdin)
	defer input_scanner.Err()

	message_buf := make([]Message, 128)

	fmt.Print("> ")

	for input_scanner.Scan() {
		raw := input_scanner.Text()
		text := strings.TrimRight(raw, "\r\n")

		if len(text) == 0 {
			fmt.Print("> ")
			continue
		}

		n_messages_to_send := int(math.Ceil(float64(len(text)) / float64(MESSAGE_DATA_SIZE)))

		messages_to_send := message_buf[:0]

		text_pos := 0
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

		fmt.Print("> ")

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

func send_message(conn *net.UDPConn, send_addr *net.UDPAddr, message Message) error {
	var buf bytes.Buffer

	// NOTE jfd 17/06/26: randomly drop messages
	if test_rng.Int()%7 == 0 {
		return nil
	}

	err := binary.Write(&buf, binary.BigEndian, message)
	if err != nil {
		return err
	}

	_, err = conn.WriteToUDP(buf.Bytes(), send_addr)

	return err
}

func exponential_moving_average(avg float64, sample float64, coeficient float64) float64 {
	result := (1.0-coeficient)*avg + coeficient*sample
	return result
}

func main() {

	var err error

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

	send_message_channel := make(chan Message, 64)
	receive_message_channel := make(chan Message, 64)

	go get_user_input(send_message_channel)
	go get_incoming_messages(conn, receive_message_channel)

	send_message_queue := make([]Message, 0, 128)
	receive_message_queue := make([]Message, 0, 128) // NOTE jfd 17/06/26: this stores the messages that arrive so that we can join them together once we have a begin and end text message
	in_flight_messages := make([]Message, 0, 128)
	in_flight_message_sent_times := make(map[uint32]time.Time)

	// NOTE jfd 19/06/26: tracking metrics for agentic congestion control
	aimd_enabled := true
	var average_rtt time.Duration
	var rtt_variance time.Duration
	original_transmission_count := 0
	retransmission_count := 0
	lost_messages := 0
	successfully_acked_messages := 0
	bytes_acked := 0
	last_time_bytes_acked_was_measured := time.Now()
	throughput_bps := 0.0
	metrics_ticker := time.NewTicker(time.Second)

	min_window_size := 1
	window_size := 4
	window_base_seq_num := 0
	next_seq_num := 0          // next sequence number to use when sending messages
	next_expected_seq_num := 0 // next sequence number expected by the receiver
	print_backing_buf := make([]byte, 1024)
	count_acks_received := 0

	var timer *time.Timer
	var timeout_channel <-chan time.Time
	timeout_duration := time.Millisecond * 2000

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

			elapsed := time.Since(last_time_bytes_acked_was_measured)
			throughput_sample := 8 * float64(bytes_acked) / elapsed.Seconds()

			throughput_bps = exponential_moving_average(throughput_bps, throughput_sample, 0.125)
			bytes_acked = 0
			last_time_bytes_acked_was_measured = time.Now()

			metrics := Metrics{
				Avg_rtt:             average_rtt,
				Rtt_variance:        rtt_variance,
				Loss_rate:           float64(lost_messages) / float64(successfully_acked_messages),
				Retransmission_rate: float64(retransmission_count) / float64(original_transmission_count),
				Throughput_bps:      throughput_bps,
				Window_size:         window_size,
			}

			log.Printf("%+v\n", metrics)

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

				if aimd_enabled {
					// NOTE jfd 18/06/26: AIMD
					window_size = max(min_window_size, window_size/2)
					count_acks_received = 0
					log.Printf("\nwindow_size = %d, congestion!!!\n", window_size)
				}

			}

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
						bytes_acked += len(message_acked.Data)

						sent_time, ok := in_flight_message_sent_times[message_acked.Seq_num]
						{

							if ok {
								sample_rtt := time.Since(sent_time)
								delete(in_flight_message_sent_times, message_acked.Seq_num)

								// NOTE jfd 19/06/26: init the average and variance
								if average_rtt == 0 {
									average_rtt = sample_rtt
									rtt_variance = average_rtt / 2
								} else {
									average_rtt = time.Duration(
										exponential_moving_average(float64(average_rtt), float64(sample_rtt), 0.125),
									)

									diff := math.Abs(float64(sample_rtt - average_rtt))

									rtt_variance = time.Duration(
										exponential_moving_average(float64(rtt_variance), diff, 0.25),
									)
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
						if aimd_enabled {
							window_size++
						}
						count_acks_received = 0
					}

					if acknowleged_any_in_flight_messages {

						log.Printf("\nwindow_size = %d, increasing...\n", window_size)

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
					send_message(conn, send_addr, ack_message)
				} else {
					ack_message := Message{
						Flags:   MESSAGE_FLAG_ACK,
						Seq_num: uint32(next_expected_seq_num) - 1,
					}
					send_message(conn, send_addr, ack_message)
				}

			}

		}

		// NOTE jfd 17/06/26: fill the window with messages
		if len(send_message_queue) > 0 {

			window_was_empty := (len(in_flight_messages) == 0)

			for len(send_message_queue) > 0 && len(in_flight_messages) < window_size {
				message := send_message_queue[0]
				send_message_queue = send_message_queue[1:]

				if err := send_message(conn, send_addr, message); err != nil {
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
