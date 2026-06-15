package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

var (
	send_port   int
	listen_port int
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

var test_rng *rand.Rand

func read_user_input(send_message chan<- Message) {
	scanner := bufio.NewScanner(os.Stdin)

	buf := make([]byte, 1024)
	scanner.Buffer(buf, 1024*1024)

	message_buf := make([]Message, 512)

	fmt.Print("> ")

	for scanner.Scan() {

		text := scanner.Text()
		line := strings.TrimRight(text, "\r\n")

		n_messages_to_transmit := int(math.Ceil(float64(len(line)) / float64(MESSAGE_DATA_SIZE)))

		messages_to_transmit := message_buf[:n_messages_to_transmit]
		pos_in_line := 0
		for i := 0; i < n_messages_to_transmit; i++ {
			m := &messages_to_transmit[i]
			*m = Message{}

			for j := 0; j < len(m.Data) && pos_in_line < len(line); j++ {
				m.Data[j] = line[pos_in_line]
				pos_in_line++
			}

		}

		messages_to_transmit[0].Flags |= MESSAGE_FLAG_BEGIN_TEXT
		messages_to_transmit[n_messages_to_transmit-1].Flags |= MESSAGE_FLAG_END_TEXT

		for _, m := range messages_to_transmit {
			send_message <- m
		}

		fmt.Print("> ")

	}

}

func send_message(conn *net.UDPConn, send_addr *net.UDPAddr, message Message) error {
	var buf bytes.Buffer

	// NOTE jfd 15/06/26: randomly drop messages here to make sure the acks work
	if test_rng.Int() % 2 == 0 {
		return nil
	}

	err := binary.Write(&buf, binary.BigEndian, message)

	if err != nil {
		return err
	}

	data := buf.Bytes()
	_, err = conn.WriteToUDP(data, send_addr)

	return err
}

func read_incoming_messages(conn *net.UDPConn, recv_message chan<- Message) {
	buf := make([]byte, 65535)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		var message Message
		if err := binary.Read(bytes.NewReader(buf[:n]), binary.BigEndian, &message); err != nil {
			continue
		}

		recv_message <- message

	}

}

func main() {

	// rand_source := rand.NewSource(42)
	rand_source := rand.NewSource(time.Now().UnixNano())
	test_rng = rand.New(rand_source)

	window_size := uint32(4)
	retransmission_timeout := 1500*time.Millisecond

	listen_addr_flag := flag.String("listen", ":9001", "UDP address to listen on")
	send_addr_flag := flag.String("send", "127.0.0.1:9002", "UDP address to send to")
	flag.Parse()

	listen_addr, err := net.ResolveUDPAddr("udp", *listen_addr_flag)
	if err != nil {
		fmt.Println("resolve listen addr:", err)
		return
	}

	send_addr, err := net.ResolveUDPAddr("udp", *send_addr_flag)
	if err != nil {
		fmt.Println("resolve send addr:", err)
		return
	}

	conn, err := net.ListenUDP("udp", listen_addr)
	if err != nil {
		fmt.Println("listen udp:", err)
		return
	}
	defer conn.Close()

	fmt.Printf("listening on %s, sending to %s\n", listen_addr, send_addr)

	send_message_channel := make(chan Message, 64)
	recv_message_channel := make(chan Message, 64)

	echo_buffer := make([]byte, 4096)

	go read_user_input(send_message_channel)
	go read_incoming_messages(conn, recv_message_channel)

	var (
		send_queue          []Message
		receive_buffer      []Message
		in_flight           []Message
		window_base_seq_num uint32
		next_seq_num        uint32
		expected_seq_num    uint32

		timer *time.Timer
		timeout_channel <-chan time.Time
	)

	stop_timer := func() {
		if timer == nil {
			return
		}

		if !timer.Stop() {
			select {
				case <-timer.C:
				default:
			}
		}

		timer = nil
		timeout_channel = nil
	}

	start_or_reset_timer := func() {
		if len(in_flight) == 0 {
			stop_timer()
			return
		}

		// start
		if timer == nil {
			timer = time.NewTimer(retransmission_timeout)
			timeout_channel = timer.C
			return
		}

		if !timer.Stop() {
			select {
				case <-timer.C:
				default:
			}
		}

		timer.Reset(retransmission_timeout)
		timeout_channel = timer.C
	}

	fill_window := func() {
		window_was_empty := (len(in_flight) == 0)

		for len(send_queue) > 0 && uint32(len(in_flight)) < window_size {
			message := send_queue[0]
			send_queue = send_queue[1:]

			// NOTE jfd 15/06/26: Seq_num isn't set by read_user_input
			message.Seq_num = next_seq_num

			if err := send_message(conn, send_addr, message); err != nil {
				fmt.Fprintln(os.Stderr, "error when sending packet:", err)
				send_queue = append([]Message{message}, send_queue...)
				break
			}

			in_flight = append(in_flight, message)
			next_seq_num++

		}

		if window_was_empty && len(in_flight) > 0 {
			start_or_reset_timer()
		}

	}

	for {

		received_end_text_message := false

		select {

		case <-timeout_channel:
			for _,message := range in_flight {
				if err := send_message(conn, send_addr, message); err != nil {
					fmt.Fprintln(os.Stderr, "error on retransmit:", err)
				}
			}
			start_or_reset_timer()

		case message_to_send := <-send_message_channel:
			send_queue = append(send_queue, message_to_send)
			fill_window()

		case message_to_recv := <-recv_message_channel:

			// handle ack
			if message_to_recv.Flags&MESSAGE_FLAG_ACK != 0 {

				ack_num := message_to_recv.Seq_num

				if ack_num > window_base_seq_num && ack_num <= next_seq_num {
					acknowleged_any_in_flight_messages := false

					for len(in_flight) > 0 && in_flight[0].Seq_num < ack_num {
						in_flight = in_flight[1:]
						window_base_seq_num++
						acknowleged_any_in_flight_messages = true
					}

					if acknowleged_any_in_flight_messages {
						if len(in_flight) == 0 {
							stop_timer()
						} else {
							start_or_reset_timer()
						}

						fill_window()

					}

				}

			} else {
				// is normal data message

				if message_to_recv.Seq_num == expected_seq_num {
					if message_to_recv.Flags&MESSAGE_FLAG_BEGIN_TEXT != 0 {
						receive_buffer = receive_buffer[:0]
					}
					receive_buffer = append(receive_buffer, message_to_recv)
					expected_seq_num++
					if message_to_recv.Flags&MESSAGE_FLAG_END_TEXT != 0 {
						received_end_text_message = true
					}
				}

				ack := Message{
					Flags:   MESSAGE_FLAG_ACK,
					Seq_num: expected_seq_num,
				}

				send_message(conn, send_addr, ack)

			}

		}

		if len(send_queue) > 0 {
			fill_window()
		}

		if received_end_text_message {
			joined_bytes := echo_buffer[0:]

			for _,m := range receive_buffer {
				for i := 0; i < len(m.Data); i++ {
					if m.Data[i] != 0 {
						joined_bytes = append(joined_bytes, m.Data[i])
					}
				}
			}

			reconstructed_message := string(joined_bytes)

			fmt.Printf("\npeer@%s: %s\n", send_addr, reconstructed_message)
			fmt.Printf("> ")

		}

	}

}
