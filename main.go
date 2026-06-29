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

const SEGMENT_DATA_SIZE = 8

type Segment struct {
	Flags      uint32
	Seq_num    uint32
	Data_len   uint32
	Data       [SEGMENT_DATA_SIZE]byte
}

type LLM_input struct {
	Avg_rtt                           float64 `json:"avg_rtt_us"`
	Rtt_variance                      float64 `json:"rtt_variance_us"`

	Throughput_ema_bps    						float64 `json:"throughput_ema_bps"`
	Prev_throughput_ema_bps    				float64 `json:"prev_throughput_ema_bps"`
	Retransmission_ratio_ema 	   	    float64 `json:"retransmission_ratio_ema"`
	Prev_retransmission_ratio_ema 	  float64 `json:"prev_retransmission_ratio_ema"`

	Throughput_sample_bps    				  float64 `json:"throughput_sample_bps"`
	Prev_throughput_sample_bps        float64 `json:"prev_throughput_sample_bps"`
	Retransmission_ratio_sample 	   	float64 `json:"retransmission_ratio_sample"`
	Prev_retransmission_ratio_sample 	float64 `json:"prev_retransmission_ratio_sample"`

	Throughput_sample_history    			[]float64  `json:"throughput_sample_history"`

	Total_bytes_transmitted           int     `json:"total_bytes_transmitted"`
	Cwnd                              int     `json:"cwnd"`
	Ssthresh                          int     `json:"ssthresh"`
	Prev_cwnd                         int     `json:"prev_cwnd"`
	Prev_ssthresh                     int     `json:"prev_ssthresh"`
}

type LLM_input_sample struct {
	Avg_rtt                           float64 `json:"avg_rtt_us"`
	Rtt_variance                      float64 `json:"rtt_variance_us"`
	Throughput_sample_bps    				  float64 `json:"throughput_sample_bps"`
	Prev_throughput_sample_bps        float64 `json:"prev_throughput_sample_bps"`
	Retransmission_ratio_sample 	   	float64 `json:"retransmission_ratio_sample"`
	Prev_retransmission_ratio_sample 	float64 `json:"prev_retransmission_ratio_sample"`
	Total_bytes_transmitted           int     `json:"total_bytes_transmitted"`
	Cwnd                              int     `json:"cwnd"`
	Ssthresh                          int     `json:"ssthresh"`
	Prev_cwnd                         int     `json:"prev_cwnd"`
	Prev_ssthresh                     int     `json:"prev_ssthresh"`
}

type LLM_input_ema struct {
	Avg_rtt                           float64 `json:"avg_rtt_us"`
	Rtt_variance                      float64 `json:"rtt_variance_us"`
	Throughput_ema_bps    						float64 `json:"throughput_ema_bps"`
	Retransmission_ratio_ema 	   	    float64 `json:"retransmission_ratio_ema"`
	Total_bytes_transmitted           int     `json:"total_bytes_transmitted"`
	Cwnd                              int     `json:"cwnd"`
	Ssthresh                          int     `json:"ssthresh"`
	Prev_cwnd                         int     `json:"prev_cwnd"`
	Prev_ssthresh                     int     `json:"prev_ssthresh"`
}

type LLM_input_throughput_history struct {
	Avg_rtt                           float64 `json:"avg_rtt_us"`
	Throughput_sample_history    			[]float64   `json:"throughput_sample_history"`
	Cwnd                              int     `json:"cwnd"`
	Ssthresh                          int     `json:"ssthresh"`
}

type Profile_metrics struct {
	Avg_rtt                           float64 `json:"avg_rtt_micro_seconds"`
	Rtt_variance                      float64 `json:"rtt_variance_micro_seconds"`
	EMA_throughput_bps                float64 `json:"ema_throughput_bps"`
	EMA_retransmission_ratio_bps      float64 `json:"ema_retransmission_ratio_bps"`
	Raw_throughput_bps                float64 `json:"raw_throughput_bps"`
	Raw_retransmission_ratio_bps      float64 `json:"raw_retransmission_ratio_bps"`
	Timeout_interval_milliseconds     float64 `json:"timeout_interval_milliseconds"`
	Acked_bytes                       int     `json:"acked_bytes"`
	Retransmitted_bytes               int     `json:"total_bytes_retransmitted"`
	Window_size                       int     `json:"window_size"`
	Slow_start_threshold              int     `json:"slow_start_threshold"`
}

type LLM_output struct {
	Request_error              error   `json:"-"`
	Tokens_used                int     `json:"-"`
	New_cwnd                   int     `json:"new_cwnd"`
	New_ssthresh               int     `json:"new_ssthresh"`
	New_timeout_interval       float64 `json:"new_timeout_interval"`
}

var (
	LLM_ENABLED                 string
	TEST_MODE_ENABLED           string
	TEST_TRAFFIC_BITRATE_BPS    string
	TEST_TRAFFIC_DELAY_INTERVAL string
	TEST_TRAFFIC_BITS_TO_SEND   string
	PROFILE_METRICS_NAME_PREFIX string
	PROFILE_INTERVAL            string
	LLM_INPUT_MODE              string
	LLM_OUTPUT_MODE             string
)

const (
	LLM_INPUT_MODE_SAMPLE = "sample"
	LLM_INPUT_MODE_EMA = "ema"
	LLM_INPUT_MODE_THROUGHPUT_HISTORY = "throughput_history"

	LLM_OUTPUT_MODE_NORMAL = ""
	LLM_OUTPUT_MODE_WITH_TIMEOUT = "with_timeout"
)

func main() {

	var err error

	err = godotenv.Load()

	var (
		test_traffic_delay_interval time.Duration
		test_traffic_bits_to_send int
		test_traffic_bitrate_bps int
		profile_llm_input_name_prefix string
		profile_interval time.Duration
	)

	LLM_ENABLED                 = os.Getenv("LLM_ENABLED")
	TEST_MODE_ENABLED           = os.Getenv("TEST_MODE_ENABLED")
	TEST_TRAFFIC_BITRATE_BPS    = os.Getenv("TEST_TRAFFIC_BITRATE_BPS")
	TEST_TRAFFIC_DELAY_INTERVAL = os.Getenv("TEST_TRAFFIC_DELAY_INTERVAL")
	TEST_TRAFFIC_BITS_TO_SEND   = os.Getenv("TEST_TRAFFIC_BITS_TO_SEND")
	PROFILE_METRICS_NAME_PREFIX = os.Getenv("PROFILE_METRICS_NAME_PREFIX")
	PROFILE_INTERVAL 						= os.Getenv("PROFILE_INTERVAL")
	LLM_INPUT_MODE              = os.Getenv("LLM_INPUT_MODE")
	LLM_OUTPUT_MODE             = os.Getenv("LLM_OUTPUT_MODE")

	if LLM_INPUT_MODE == "" {
		LLM_INPUT_MODE = LLM_INPUT_MODE_SAMPLE
	}

	if LLM_INPUT_MODE != LLM_INPUT_MODE_SAMPLE && LLM_INPUT_MODE != LLM_INPUT_MODE_EMA && LLM_INPUT_MODE != LLM_INPUT_MODE_THROUGHPUT_HISTORY {
		panic("LLM_INPUT_MODE must be 'sample' or 'ema' or 'throughput_history'")
	}

	if LLM_OUTPUT_MODE != "" && LLM_OUTPUT_MODE != "timeout" {
		panic("LLM_OUTPUT_MODE must be 'normal' or 'timeout' or none")
	}

	if TEST_TRAFFIC_DELAY_INTERVAL != "" {
		test_traffic_delay_interval, err = time.ParseDuration(TEST_TRAFFIC_DELAY_INTERVAL)
		if err != nil { panic(err) }
	}

	if TEST_TRAFFIC_BITS_TO_SEND != "" {
		test_traffic_bits_to_send, err = strconv.Atoi(TEST_TRAFFIC_BITS_TO_SEND)
		if err != nil { panic(err) }
	}

	if TEST_TRAFFIC_BITRATE_BPS != "" {
		test_traffic_bitrate_bps, err = strconv.Atoi(TEST_TRAFFIC_BITRATE_BPS)
		if err != nil { panic(err) }
		if test_traffic_bitrate_bps == 0 {}
	}

	if PROFILE_METRICS_NAME_PREFIX == "" {
		profile_llm_input_name_prefix = "llm_input_"
	} else {
		profile_llm_input_name_prefix = PROFILE_METRICS_NAME_PREFIX
	}

	if PROFILE_INTERVAL != "" {
		profile_interval, err = time.ParseDuration(PROFILE_INTERVAL)
		if err != nil { panic(err) }
	} else {
		profile_interval = time.Second * 1
	}

	profile_llm_input_csv_path := fmt.Sprintf("%s%d.csv", profile_llm_input_name_prefix, os.Getpid())

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

	llm_output_channel := make(chan LLM_output, 3)

	send_segment_queue := make([]Segment, 0, 128)
	receive_segment_queue := make([]Segment, 0, 128) // NOTE 17/06/26: this stores the segments that arrive so that we can join them together once we have a begin and end text segment
	in_flight_segments := make([]Segment, 0, 128)
	min_window_size := 1
	window_size := min_window_size
	prev_window_size := window_size
	min_slow_start_threshold := 2
	slow_start_threshold := min_slow_start_threshold
	prev_slow_start_threshold := slow_start_threshold
	count_in_order_acks_received := 0
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

	var groq_client *groq.Client
	if llm_enabled {
		groq_client = groq.NewClient()
	}

	if test_mode_enabled {
		go generate_test_traffic(test_traffic_bits_to_send, test_traffic_delay_interval, send_segment_channel)
	} else {
		go get_user_input(send_segment_channel)
	}

	go get_incoming_segments(conn, receive_segment_channel)

	// NOTE 19/06/26: tracking input for profiling and agentic congestion control
	average_rtt := 0.0
	rtt_variance := 0.0
	throughput_ema := 0.0
	acked_bytes := 0
	retransmission_ratio_ema := 0.0
	throughput_sample := 0.0
	retransmission_ratio_sample := 0.0
	total_bytes_retransmitted := 0
	total_bytes_transmitted := 0
	in_flight_segment_sent_times := make(map[uint32]time.Time)
	profile_llm_input_ticker_duration := profile_interval
	profile_llm_input_ticker := time.NewTicker(profile_llm_input_ticker_duration)
	throughput_sample_history := make([]float64, 0, 4)

	raw_ack_threshold_computed := false
	count_raw_acks_received := 0
	raw_ack_threshold := 0
	raw_ack_count_ticker := time.NewTicker(time.Second * 10)
	call_llm := false
	last_llm_call_time := time.Now()

	// NOTE 17/06/26: this timer stuff in go is very confusing
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

		case <-raw_ack_count_ticker.C:
			raw_ack_threshold_computed = true
			raw_ack_threshold = count_raw_acks_received / 10

		case <-profile_llm_input_ticker.C:

			prev_throughput_ema := throughput_ema
			prev_retransmission_ratio_ema := retransmission_ratio_ema

			prev_throughput_sample := throughput_sample
			prev_retransmission_ratio_sample := retransmission_ratio_sample

			throughput_sample = float64(acked_bytes)*8 / float64(profile_llm_input_ticker_duration.Seconds())
			retransmission_ratio_sample = float64(total_bytes_retransmitted) / float64(total_bytes_transmitted)
			if total_bytes_transmitted <= 0 {
				retransmission_ratio_sample = 0
			}
			alpha := 0.125
			throughput_ema = exponential_moving_average(throughput_ema, throughput_sample, alpha)
			retransmission_ratio_ema = exponential_moving_average(retransmission_ratio_ema, retransmission_ratio_sample, alpha)

			if len(throughput_sample_history) == cap(throughput_sample_history) {
				throughput_sample_history = throughput_sample_history[:len(throughput_sample_history) - 1]
			}
			throughput_sample_history = append([]float64{ throughput_sample }, throughput_sample_history...)

			input := Profile_metrics{
				Avg_rtt:                           average_rtt,
				Rtt_variance:                      rtt_variance,
				EMA_throughput_bps:                throughput_ema,
				EMA_retransmission_ratio_bps:      retransmission_ratio_ema,
				Raw_throughput_bps:                throughput_sample,
				Raw_retransmission_ratio_bps:      retransmission_ratio_sample,
				Acked_bytes: 									     acked_bytes,
				Retransmitted_bytes: 			         total_bytes_retransmitted,
				Timeout_interval_milliseconds:     float64(timeout_duration) / float64(time.Millisecond),
				Window_size:                       window_size,
				Slow_start_threshold:              slow_start_threshold,
			}

			go func() {
				dump_llm_input_to_csv(profile_llm_input_csv_path, input)
			}()

			acked_bytes = 0
			total_bytes_retransmitted = 0
			total_bytes_transmitted = 0

			if call_llm && llm_enabled {
				call_llm = false

				llm_metrics := LLM_input{
					Avg_rtt: average_rtt,
					Rtt_variance: rtt_variance,

					Throughput_ema_bps: throughput_ema,
					Prev_throughput_ema_bps: prev_throughput_ema,
					Retransmission_ratio_ema: retransmission_ratio_ema,
					Prev_retransmission_ratio_ema: prev_retransmission_ratio_ema,

					Throughput_sample_bps: throughput_sample,
					Prev_throughput_sample_bps: prev_throughput_sample,
					Retransmission_ratio_sample: retransmission_ratio_sample,
					Prev_retransmission_ratio_sample: prev_retransmission_ratio_sample,

					Throughput_sample_history: throughput_sample_history,

					Total_bytes_transmitted: total_bytes_transmitted,
					Cwnd: window_size,
					Ssthresh: slow_start_threshold,
					Prev_cwnd: prev_window_size,
					Prev_ssthresh: prev_slow_start_threshold,
				}

				if time.Since(last_llm_call_time) > time.Second * 7 {
					last_llm_call_time = time.Now()
					log.Printf("calling llm\n")
					go run_llm(groq_client, llm_metrics, llm_output_channel)
				} else {
					call_llm = true
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
					total_bytes_retransmitted += int(m.Data_len)
					delete(in_flight_segment_sent_times, m.Seq_num)
				}

				in_flight_segments = in_flight_segments[:0]

				// NOTE 18/06/26: reset window and update slow_start_threshold
				slow_start_threshold = max(int(float64(window_size) * window_decrease_factor), min_slow_start_threshold)
				window_size = min_window_size
				count_in_order_acks_received = 0

				log.Printf("window_size = %d, congestion!!!\n", window_size)

			}

		case output := <-llm_output_channel:
			log.Printf("llm output! cur_cwnd: %d, cur_ssthresh: %d, new_cwnd: %d, new_ssthresh: %d\n", window_size, slow_start_threshold, output.New_cwnd, output.New_ssthresh)
			// cowabunga
			if output.Request_error != nil {
				panic(output.Request_error)
			}
			window_size = max(min_window_size, output.New_cwnd)
			slow_start_threshold = max(min_slow_start_threshold, output.New_ssthresh)

		case segment_to_send := <-send_segment_channel:
			segment_to_send.Seq_num = uint32(next_seq_num)
			next_seq_num++
			send_segment_queue = append(send_segment_queue, segment_to_send)

		case segment_received := <-receive_segment_channel:
			is_ack := (segment_received.Flags&SEGMENT_FLAG_ACK != 0)

			if is_ack {

				if raw_ack_threshold_computed {
					count_raw_acks_received++
					if count_raw_acks_received >= raw_ack_threshold {
						call_llm = true
						count_raw_acks_received = 0
					}
				}

				ack_num := int(segment_received.Seq_num)

				ack_is_within_the_window := (ack_num >= window_base_seq_num && ack_num <= next_seq_num)

				if ack_is_within_the_window {
					acknowleged_any_in_flight_segments := false

					// NOTE 23/06/26: cumulatively acknowledge segments
					for len(in_flight_segments) > 0 && in_flight_segments[0].Seq_num <= uint32(ack_num) {

						segment_acked := in_flight_segments[0]
						acked_bytes += int(segment_acked.Data_len)

						sent_time, ok := in_flight_segment_sent_times[segment_acked.Seq_num]
						{ // track_average_rtt_and_variance

							if ok {
								sample_rtt := float64(time.Since(sent_time)) / float64(time.Microsecond)
								delete(in_flight_segment_sent_times, segment_acked.Seq_num)

								// NOTE 19/06/26: init the average and variance
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
							count_in_order_acks_received = 0
							window_size += window_increase_amount
						} else {
							count_in_order_acks_received++
						}

					}

					if count_in_order_acks_received >= window_size {
						// NOTE 18/06/26: AIMD
						window_size += window_increase_amount
						count_in_order_acks_received = 0
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

		// NOTE 17/06/26: fill the window with segments
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

				total_bytes_transmitted += int(segment.Data_len)

				in_flight_segment_sent_times[segment.Seq_num] = time.Now()

				in_flight_segments = append(in_flight_segments, segment)
			}

			if window_was_empty && len(in_flight_segments) > 0 {
				start_or_reset_timer()
			}

		}

		if received_end_text_segment && !test_mode_enabled {
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

func generate_test_traffic(bits_to_send int, delay_interval time.Duration, send_segment_channel chan<- Segment) {

	rand_source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(rand_source)

	s := "macaco"

	min_random_text_len := 1
	max_random_text_len := (bits_to_send>>3)/len(s)

	for {
		random_text := strings.Repeat(s, max(min_random_text_len, rng.Intn(max_random_text_len)))
		log.Printf("sending test segment '%s'\n", random_text)

		send_text_message(random_text, send_segment_channel)
		time.Sleep(delay_interval)
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

func run_llm(client *groq.Client, input LLM_input, llm_output_channel chan<- LLM_output) {

	var system_prompt string
	var err error
	var llm_input_json []byte

	// NOTE 29/06/26: Different prompts and parameter combinations based on a predefined input mode
	log.Printf("calling llm in '%s' mode\n", LLM_INPUT_MODE)

	switch LLM_INPUT_MODE {
	case LLM_INPUT_MODE_THROUGHPUT_HISTORY:
		system_prompt =
`You are a congestion-control decision engine for a single TCP-like flow.

Input is one JSON object with:
avg_rtt_us, throughput_sample_history,
cwnd, ssthresh,

All cwnd and ssthresh values are in segments of a fixed size, not bytes. One segment means one send unit from the Segment struct. Do not convert to bytes.
cwnd is the current congestion window: how many segments may be in flight.
ssthresh is the slow-start threshold: above this point, growth should be cautious. It should reflect the last safe window after congestion, and it must never be less than 2 segments.
'throughput_sample_history' contains, at most, the last 4 samples of throughput in bps, most recent first. The throughput samples are taken every second.

Goal: choose the next congestion window and slow-start threshold for the next interval.

Rules:
- Output JSON only, with exactly these integer fields: {"new_cwnd": <int>, "new_ssthresh": <int>}
- cwnd is an integer number of segments.
- ssthresh is an integer number of segments.
- Do not drop cwnd abruptly to 1 unless all 4 of the last throughput samples have been decreasing drastically.
- When congestion is detected, set ssthresh to the new cwnd you choose, but never below 2.
- If RTT is elevated and throughput is not meaningfully improving, treat it as queue buildup and back off noticeably, but not to the minimum in one step.
- If cwnd and throughput are improving and RTT is falling, you may increase cwnd additively.
- Prefer small, stable adjustments unless the signal is strong.
- If cwnd is below 10 segments and the path does not look congested, recover aggressively toward at least 10 segments.
- Preserve fairness: if RTT is rising but cwnd is not decreasing, avoid overreacting; back off moderately, not excessively.
- Never explain your reasoning. Never output extra keys or text.`

		llm_input_sample := LLM_input_throughput_history{
			Avg_rtt: input.Avg_rtt,
			Throughput_sample_history: input.Throughput_sample_history,
			Cwnd: input.Cwnd,
			Ssthresh: input.Ssthresh,
		}

		llm_input_json, err = json.Marshal(llm_input_sample)

	default:
	case LLM_INPUT_MODE_SAMPLE:
		system_prompt =
`You are a congestion-control decision engine for a single TCP-like flow.

Input is one JSON object with:
avg_rtt_us, rtt_variance_us, throughput_sample_bps, prev_throughput_sample_bps,
retransmission_ratio_sample, prev_retransmission_ratio_sample, total_bytes_transmitted,
cwnd, ssthresh, prev_cwnd, prev_ssthresh.

All cwnd and ssthresh values are in segments of a fixed size, not bytes. One segment means one send unit from the Segment struct. Do not convert to bytes.
cwnd is the current congestion window: how many segments may be in flight.
ssthresh is the slow-start threshold: above this point, growth should be cautious. It should reflect the last safe window after congestion, and it must never be less than 2 segments.
The samples of throughput and retransmission ratio are taken every second.

Goal: choose the next congestion window and slow-start threshold for the next interval.

Rules:
- Output JSON only, with exactly these integer fields: {"new_cwnd": <int>, "new_ssthresh": <int>}
- cwnd is an integer number of segments.
- ssthresh is an integer number of segments.
- Do not drop cwnd abruptly to 1 unless loss is severe.
- If total_bytes_transmitted is 0 but the throughput is still high that most likely means congestion collapse.
- If retransmissions are present or RTT rises while throughput is flat or falling, treat it as congestion and decrease cwnd multiplicatively.
- When congestion is detected, set ssthresh to the new cwnd you choose, but never below 2.
- If RTT is elevated and throughput is not meaningfully improving, treat it as queue buildup and back off noticeably, but not to the minimum in one step.
- If cwnd and throughput are improving and RTT is falling, you may increase cwnd additively.
- Prefer small, stable adjustments unless the signal is strong.
- If cwnd is below 10 segments and the path does not look congested, recover aggressively toward at least 10 segments.
- Preserve fairness: if RTT is rising but cwnd is not decreasing, avoid overreacting; back off moderately, not excessively.
- Use the recent trend implied by current and previous values, not a single noisy sample.
- Never explain your reasoning. Never output extra keys or text.`

		llm_input_sample := LLM_input_sample{
			Avg_rtt: input.Avg_rtt,
			Rtt_variance: input.Rtt_variance,
			Throughput_sample_bps: input.Throughput_sample_bps,
			Prev_throughput_sample_bps: input.Prev_throughput_sample_bps,
			Retransmission_ratio_sample: input.Retransmission_ratio_sample,
			Prev_retransmission_ratio_sample: input.Prev_retransmission_ratio_sample,
			Total_bytes_transmitted: input.Total_bytes_transmitted,
			Cwnd: input.Cwnd,
			Ssthresh: input.Ssthresh,
			Prev_cwnd: input.Prev_cwnd,
			Prev_ssthresh: input.Prev_ssthresh,
		}

		llm_input_json, err = json.Marshal(llm_input_sample)

	case LLM_INPUT_MODE_EMA:
		system_prompt =
`You are a congestion-control decision engine for a single TCP-like flow.

Input is one JSON object with:
avg_rtt_us, rtt_variance_us, throughput_ema_bps,
retransmission_ratio_ema, total_bytes_transmitted,
cwnd, ssthresh, prev_cwnd, prev_ssthresh.

All cwnd and ssthresh values are in segments of a fixed size, not bytes. One segment means one send unit from the Segment struct. Do not convert to bytes.
cwnd is the current congestion window: how many segments may be in flight.
ssthresh is the slow-start threshold: above this point, growth should be cautious. It should reflect the last safe window after congestion, and it must never be less than 2 segments.

Goal: choose the next congestion window and slow-start threshold for the next interval.

Rules:
- Output JSON only, with exactly these integer fields: {"new_cwnd": <int>, "new_ssthresh": <int>}
- cwnd is an integer number of segments.
- ssthresh is an integer number of segments.
- Do not drop cwnd abruptly to 1 unless loss is severe.
- If total_bytes_transmitted is 0 but the throughput is still high that most likely means congestion collapse.
- If retransmissions are present or RTT rises while throughput is flat or falling, treat it as congestion and decrease cwnd multiplicatively.
- When congestion is detected, set ssthresh to the new cwnd you choose, but never below 2.
- If RTT is elevated and throughput is not meaningfully improving, treat it as queue buildup and back off noticeably, but not to the minimum in one step.
- If cwnd and throughput are improving and RTT is falling, you may increase cwnd additively.
- Prefer small, stable adjustments unless the signal is strong.
- If cwnd is below 10 segments and the path does not look congested, recover aggressively toward at least 10 segments.
- Preserve fairness: if RTT is rising but cwnd is not decreasing, avoid overreacting; back off moderately, not excessively.
- Never explain your reasoning. Never output extra keys or text.`

		llm_input_ema := LLM_input_ema{
			Avg_rtt: input.Avg_rtt,
			Rtt_variance: input.Rtt_variance,
			Throughput_ema_bps: input.Throughput_ema_bps,
			Retransmission_ratio_ema: input.Retransmission_ratio_ema,
			Total_bytes_transmitted: input.Total_bytes_transmitted,
			Cwnd: input.Cwnd,
			Ssthresh: input.Ssthresh,
			Prev_cwnd: input.Prev_cwnd,
			Prev_ssthresh: input.Prev_ssthresh,
		}

		llm_input_json, err = json.Marshal(llm_input_ema)
	}

	if err != nil {
		return
	}

	groq_messages := []groq.Message{
		{
			Role:    "system",
			Content: system_prompt,
		},
		{
			Role:    "user",
			Content: string(llm_input_json),
		},
	}

	response, err := client.CreateChatCompletion(groq.CompletionCreateParams{
		Model:    "llama-3.1-8b-instant",
		Messages: groq_messages,
		ResponseFormat: groq.ResponseFormat{
			Type: "json_object",
		},
	})

	var output LLM_output

	if err != nil {
		output.Request_error = err
		llm_output_channel <- output
		return
	}


	if response.Usage.TotalTokens != nil {
		output.Tokens_used = *response.Usage.TotalTokens
	}

	if len(response.Choices) > 0 {
		content := response.Choices[0].Message.Content

		err := json.Unmarshal([]byte(content), &output)
		if err != nil {
			return
		}

	}

	llm_output_channel <- output

}

func exponential_moving_average(avg float64, sample float64, coefficient float64) float64 {
	result := (1.0-coefficient)*avg + coefficient*sample
	return result
}

func dump_llm_input_to_csv(path string, m Profile_metrics) error {
	_, err := os.Stat(path)
	fileExists := err == nil

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	t := reflect.TypeOf(m)
	v := reflect.ValueOf(m)

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

	if !fileExists {
		if err := w.Write(header); err != nil {
			return err
		}
	}

	if err := w.Write(row); err != nil {
		return err
	}

	w.Flush()
	return w.Error()
}

