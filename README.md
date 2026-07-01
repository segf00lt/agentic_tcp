# agentic_tcp

A Go-Back-N reliable data transfer protocol implementation with LLM assisted congestion
control, in the form of a CLI peer-to-peer text messaging application.

## Build and Usage

If you don't have Go installed already, run `./install_go.sh`.

With Go installed build with `go build -mod=vendor`, then you can run `./agentic_tcp -listen :9001 -send :9002` which
will start an instance of the program listening on `localhost:9001` and sending on `localhost:9002`. Run another instance
in a separate terminal with the ports swapped and voila, you can try sending messages to yourself.

To enable the LLM, set the environment `LLM_ENABLED=1` and `GROQ_API_KEY` to a valid Groq API key. This will enable a
free `llama-3.1-8b-instant` model.

## Running Experiments

To run the experiment simply run `sudo ./test.sh`.

Root privileges are required for running `tc` in order to simulate different network conditions.
