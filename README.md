# agentic_tcp

A Go-Back-N reliable data transfer protocol implementation with LLM assisted congestion
control, in the form of a CLI peer-to-peer text messaging application.

## Build and Usage

If you don't have Go installed already, run `./install_go.sh`.

With Go installed build with `go build -mod=vendor`.

To do an interactive test, open to separate terminals and in one run:

```
./agentic_tcp -listen :9001 -send :9002
```

and in the other

```
./agentic_tcp -listen :9002 -send :9001
```

and try sending yourself some messages!

Want to see if the Go-Back-N implementation even works? Before running the above commands, run `sudo ./netem.sh mobile` in one
of your terminals. This will use `tc` to apply simulated loss, delay and packet reordering to `localhost`. You should notice some
messages (especially long ones) take a little more time to arrive.

To enable the LLM, set the environment variables `LLM_ENABLED=1` and `GROQ_API_KEY=valid_groq_api_key` before running each instance.
This will use a free `llama-3.1-8b-instant` instance as the model. Due to how the LLM is triggered, it's unlikely that it will
even be called when running in the interactive mode due to the low amount of traffic.

## Running Experiments

To run the experiment simply run `sudo ./test.sh`.

Root privileges are required for running `tc` in order to simulate different network conditions.
