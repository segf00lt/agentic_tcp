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
	"encoding/csv"
	"reflect"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/jpoz/groq"
)

const (
	SEGMENT_FLAG_ACK                = 1 << iota
	SEGMENT_FLAG_BEGIN_MESSAGE_TEXT = 1 << iota
	SEGMENT_FLAG_END_MESSAGE_TEXT   = 1 << iota
)

const SEGMENT_DATA_SIZE = 128

type Segment struct {
	Flags      uint32
	Seq_num    uint32
	Data_len   uint32
	Data       [SEGMENT_DATA_SIZE]byte
}

type Metrics struct {
	Avg_rtt                           float64 `json:"avg_rtt_micro_seconds"`
	Rtt_variance                      float64 `json:"rtt_variance_micro_seconds"`
	EMA_throughput_bits_per_second    float64 `json:"ema_throughput_bits_per_second"`
	EMA_retransmitted_bits_per_second float64 `json:"ema_retransmitted_bits_per_second"` // NOTE: basically the retransmission rate
	Raw_throughput_bits_per_second    float64 `json:"raw_throughput_bits_per_second"`
	Raw_retransmitted_bits_per_second float64 `json:"raw_retransmitted_bits_per_second"`
	Timeout_interval_milliseconds     float64 `json:"timeout_interval_milliseconds"`
	Acked_bytes                       int     `json:"acked_bytes"`
	Retransmitted_bytes               int     `json:"retransmitted_bytes"`
	Window_size                       int     `json:"window_size"`
	Slow_start_threshold              int     `json:"slow_start_threshold"`
	Window_increase_amount            int     `json:"window_increase_amount"`
	Window_decrease_factor            float64 `json:"window_decrease_factor"`
}

type Decision struct {
	New_window_increase_amount int     `json:"new_window_increase_amount"`
	New_window_decrease_factor float64 `json:"new_window_decrease_factor"`
}

func main() {

	var err error

	err = godotenv.Load()

	LLM_ENABLED := os.Getenv("LLM_ENABLED")
	TEST_MODE_ENABLED := os.Getenv("TEST_MODE_ENABLED")

	metrics_csv_path := fmt.Sprintf("metrics%d.csv", os.Getpid())

	log_file, err := os.OpenFile(fmt.Sprintf("%d.log", os.Getpid()), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer log_file.Close()

	log.SetOutput(log_file)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

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

	send_segment_channel := make(chan Segment, 128)
	receive_segment_channel := make(chan Segment, 128)

	metrics_channel := make(chan Metrics, 128)
	decision_channel := make(chan Decision, 128)

	send_segment_queue := make([]Segment, 0, 128)
	receive_segment_queue := make([]Segment, 0, 128) // NOTE jfd 17/06/26: this stores the segments that arrive so that we can join them together once we have a begin and end text segment
	in_flight_segments := make([]Segment, 0, 128)
	min_window_size := 1
	window_size := min_window_size
	min_slow_start_threshold := 2
	slow_start_threshold := min_slow_start_threshold
	count_acks_received := 0
	window_increase_amount := 1
	window_decrease_factor := 0.5
	window_base_seq_num := 0
	next_seq_num := 0          // next sequence number to use when sending segments
	next_expected_seq_num := 0 // next sequence number expected by the receiver
	print_backing_buf := make([]byte, 1<<20)
	var timer *time.Timer
	var timeout_channel <-chan time.Time
	timeout_duration := time.Millisecond * 400

	test_mode_enabled := (TEST_MODE_ENABLED == "1")
	llm_enabled := (LLM_ENABLED == "1")

	if test_mode_enabled {
		go get_test_input(send_segment_channel)
	} else {
		go get_user_input(send_segment_channel)
	}

	go get_incoming_segments(conn, receive_segment_channel)

	if llm_enabled {
		go groq_loop(metrics_channel, decision_channel)
	}

	// NOTE jfd 19/06/26: tracking metrics for agentic congestion control
	average_rtt := 0.0
	rtt_variance := 0.0
	throughput_bps := 0.0
	acked_bytes := 0
	retransmitted_bps := 0.0
	retransmitted_bytes := 0
	in_flight_segment_sent_times := make(map[uint32]time.Time)
	metrics_ticker_duration := time.Second * 1
	metrics_ticker := time.NewTicker(metrics_ticker_duration)

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

		received_end_text_segment := false

		select {

		case <-metrics_ticker.C:

			sample_throughput := float64(acked_bytes)*8 / float64(metrics_ticker_duration.Seconds())
			sample_retransmitted := float64(retransmitted_bytes)*8 / float64(metrics_ticker_duration.Seconds())
			// alpha := 0.5
			alpha := 0.125
			throughput_bps = exponential_moving_average(throughput_bps, sample_throughput, alpha)
			retransmitted_bps = exponential_moving_average(retransmitted_bps, sample_retransmitted, alpha)

			metrics := Metrics{
				Avg_rtt:                       average_rtt,
				Rtt_variance:                  rtt_variance,
				EMA_throughput_bits_per_second:    throughput_bps,
				EMA_retransmitted_bits_per_second: retransmitted_bps,
				Raw_throughput_bits_per_second:    sample_throughput,
				Raw_retransmitted_bits_per_second: sample_retransmitted,
				Acked_bytes: 									 acked_bytes,
				Retransmitted_bytes: 			     retransmitted_bytes,
				Timeout_interval_milliseconds: float64(timeout_duration) / float64(time.Millisecond),
				Window_size:                   window_size,
				Slow_start_threshold:          slow_start_threshold,
				Window_increase_amount:        window_increase_amount,
				Window_decrease_factor:        window_decrease_factor,
			}

			acked_bytes = 0
			retransmitted_bytes = 0

			current_metrics_are_valid := (average_rtt > 0.0)

			previous_decision_received := (len(decision_channel) == 0)

			go func() {
				dump_metrics_to_csv(metrics_csv_path, metrics)
			}()

			if current_metrics_are_valid && previous_decision_received {
				if llm_enabled {
					metrics_channel <- metrics
				}
			}

		case <-timeout_channel:
			if len(in_flight_segments) == 0 {
				stop_timer()
			} else {
				log.Println("TIMEOUT")

				tmp := make([]Segment, len(in_flight_segments))
				copy(tmp, in_flight_segments)
				send_segment_queue = append(tmp, send_segment_queue...)

				for _, m := range in_flight_segments {
					retransmitted_bytes += int(m.Data_len)
					delete(in_flight_segment_sent_times, m.Seq_num)
				}

				in_flight_segments = in_flight_segments[:0]

				// NOTE jfd 18/06/26: reset window and update slow_start_threshold
				slow_start_threshold = max(int(float64(window_size) * window_decrease_factor), min_slow_start_threshold)
				window_size = min_window_size
				count_acks_received = 0

				log.Printf("window_size = %d, congestion!!!\n", window_size)

			}

		case decision := <-decision_channel:
			log.Printf("agent decision: %+v", decision)
			window_increase_amount = decision.New_window_increase_amount
			window_decrease_factor = decision.New_window_decrease_factor

		case segment_to_send := <-send_segment_channel:
			segment_to_send.Seq_num = uint32(next_seq_num)
			next_seq_num++
			send_segment_queue = append(send_segment_queue, segment_to_send)

		case segment_received := <-receive_segment_channel:
			is_ack := (segment_received.Flags&SEGMENT_FLAG_ACK != 0)

			if is_ack {

				ack_num := int(segment_received.Seq_num)

				ack_is_within_the_window := (ack_num >= window_base_seq_num && ack_num <= next_seq_num)

				if ack_is_within_the_window {
					acknowleged_any_in_flight_segments := false

					// NOTE jfd 23/06/26: cumulatively acknowledge segments
					for len(in_flight_segments) > 0 && in_flight_segments[0].Seq_num <= uint32(ack_num) {

						segment_acked := in_flight_segments[0]
						acked_bytes += int(segment_acked.Data_len)

						sent_time, ok := in_flight_segment_sent_times[segment_acked.Seq_num]
						{ // track_average_rtt_and_variance

							if ok {
								sample_rtt := float64(time.Since(sent_time)) / float64(time.Microsecond)
								delete(in_flight_segment_sent_times, segment_acked.Seq_num)

								// NOTE jfd 19/06/26: init the average and variance
								if average_rtt == 0 {
									average_rtt = sample_rtt
									rtt_variance = average_rtt / 2
								} else {
									average_rtt = exponential_moving_average(float64(average_rtt), float64(sample_rtt), 0.125)
									diff := math.Abs(float64(sample_rtt - average_rtt))
									rtt_variance = exponential_moving_average(float64(rtt_variance), diff, 0.25)

									new_timeout_duration := (average_rtt + 4*rtt_variance) * float64(time.Microsecond)
									timeout_duration = time.Duration(new_timeout_duration)
								}
							}

						} // track_average_rtt_and_variance

						in_flight_segments = in_flight_segments[1:]
						window_base_seq_num++
						acknowleged_any_in_flight_segments = true
						if window_size < slow_start_threshold {
							count_acks_received = 0
							window_size += window_increase_amount
						} else {
							count_acks_received++
						}

					}

					if count_acks_received >= window_size {
						// NOTE jfd 18/06/26: AIMD
						window_size += window_increase_amount
						count_acks_received = 0
					}

					if acknowleged_any_in_flight_segments {

						log.Printf("window_size = %d, increasing...\n", window_size)

						if len(in_flight_segments) == 0 {
							stop_timer()
						} else {
							start_or_reset_timer()
						}
					}

				}
			} else {
				seq_num := int(segment_received.Seq_num)

				if seq_num == next_expected_seq_num {
					if segment_received.Flags&SEGMENT_FLAG_BEGIN_MESSAGE_TEXT != 0 {
						receive_segment_queue = receive_segment_queue[:0]
					}

					receive_segment_queue = append(receive_segment_queue, segment_received)
					next_expected_seq_num++

					if segment_received.Flags&SEGMENT_FLAG_END_MESSAGE_TEXT != 0 {
						received_end_text_segment = true
					}
					ack_segment := Segment{
						Flags:   SEGMENT_FLAG_ACK,
						Seq_num: segment_received.Seq_num,
					}
					send_segment_over_udp(conn, send_addr, ack_segment)
				} else {
					ack_segment := Segment{
						Flags:   SEGMENT_FLAG_ACK,
						Seq_num: uint32(next_expected_seq_num) - 1,
					}
					send_segment_over_udp(conn, send_addr, ack_segment)
				}

			}

		}

		// NOTE jfd 17/06/26: fill the window with segments
		if len(send_segment_queue) > 0 {

			window_was_empty := (len(in_flight_segments) == 0)

			for len(send_segment_queue) > 0 && len(in_flight_segments) < window_size {
				segment := send_segment_queue[0]
				send_segment_queue = send_segment_queue[1:]

				if err := send_segment_over_udp(conn, send_addr, segment); err != nil {
					fmt.Fprintln(os.Stderr, "error while sending segment:", err)
					send_segment_queue = append([]Segment{segment}, send_segment_queue...)
					break
				}
				in_flight_segment_sent_times[segment.Seq_num] = time.Now()

				in_flight_segments = append(in_flight_segments, segment)
			}

			if window_was_empty && len(in_flight_segments) > 0 {
				start_or_reset_timer()
			}

		}

		if received_end_text_segment {
			text_buf := print_backing_buf[:0]
			for _, m := range receive_segment_queue {
				text_buf = append(text_buf, m.Data[:m.Data_len]...)
			}

			fmt.Printf("peer@%s: %s\n", send_addr, string(text_buf))
			fmt.Print("> ")

			receive_segment_queue = receive_segment_queue[:0]

		}

	}

}

func send_text_message(text string, send_segment_channel chan<- Segment) {
	var segment_backing_buf [128]Segment

	n_segments_to_send := int(math.Ceil(float64(len(text)) / float64(SEGMENT_DATA_SIZE)))

	n_times := 1
	if n_segments_to_send > len(segment_backing_buf) {
		n_times = int(math.Ceil(float64(n_segments_to_send) / float64(len(segment_backing_buf))))
	}

	text_pos := 0
	for ; n_times > 0; n_times-- {
		segments_to_send := segment_backing_buf[:0]

		for range n_segments_to_send {

			segment := Segment{}

			for j := 0; j < SEGMENT_DATA_SIZE && text_pos < len(text); j++ {
				segment.Data_len++
				segment.Data[j] = text[text_pos]
				text_pos++
			}

			segments_to_send = append(segments_to_send, segment)
		}

		segments_to_send[0].Flags |= SEGMENT_FLAG_BEGIN_MESSAGE_TEXT
		segments_to_send[n_segments_to_send-1].Flags |= SEGMENT_FLAG_END_MESSAGE_TEXT

		for _, m := range segments_to_send {
			send_segment_channel <- m
		}
	}

}

func get_user_input(send_segment_channel chan<- Segment) {
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

		send_text_message(text, send_segment_channel)

		fmt.Print("> ")

	}
}

func get_test_input(send_segment_channel chan<- Segment) {

	rand_source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(rand_source)

	min_random_text_len := 1
	max_random_text_len := 200

	for {
		random_text := strings.Repeat("macaco", max(min_random_text_len, rng.Intn(max_random_text_len)))
		log.Printf("sending test segment '%s'\n", random_text)

		send_text_message(random_text, send_segment_channel)
		time.Sleep(time.Millisecond*10)
	}

}

func get_incoming_segments(conn *net.UDPConn, receive_segment_channel chan<- Segment) {
	buf := make([]byte, math.MaxUint16)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		var segment Segment
		if err := binary.Read(bytes.NewReader(buf[:n]), binary.BigEndian, &segment); err != nil {
			continue
		}

		receive_segment_channel <- segment
	}

}

func send_segment_over_udp(conn *net.UDPConn, send_addr *net.UDPAddr, segment Segment) error {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.BigEndian, segment)
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
		groq_segments := []groq.Message{
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
			Messages:       groq_segments,
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

func exponential_moving_average(avg float64, sample float64, coefficient float64) float64 {
	result := (1.0-coefficient)*avg + coefficient*sample
	return result
}

func dump_metrics_to_csv(path string, m Metrics) error {
	// check if file exists
	_, err := os.Stat(path)
	fileExists := err == nil

	// open file in append mode, create if missing
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	t := reflect.TypeOf(m)
	v := reflect.ValueOf(m)

	// build header + row
	header := make([]string, 0, t.NumField())
	row := make([]string, 0, t.NumField())

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		tag := field.Tag.Get("json")
		if tag == "" {
			tag = field.Name
		} else {
			tag = strings.Split(tag, ",")[0]
		}

		header = append(header, tag)

		val := v.Field(i)

		switch val.Kind() {
		case reflect.Float64:
			row = append(row, strconv.FormatFloat(val.Float(), 'f', -1, 64))
		case reflect.Int, reflect.Int64, reflect.Int32:
			row = append(row, strconv.FormatInt(val.Int(), 10))
		default:
			row = append(row, "")
		}
	}

	// write header only if file didn't exist
	if !fileExists {
		if err := w.Write(header); err != nil {
			return err
		}
	}

	// always append row
	if err := w.Write(row); err != nil {
		return err
	}

	w.Flush()
	return w.Error()
}

