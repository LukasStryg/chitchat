# Assignment 3: Chit Chat

### How to run

All commands are run from the project root.

Start the server:

```
go run ./server
```

Start each client in its own terminal, with a unique name:

```
go run ./client Alice
go run ./client Bob
go run ./client Charlie
```

In a client:
- Type a message and press Enter to publish it.
- Type `leave` or press Ctrl+C to leave the chat.

Press Ctrl+C in the server terminal to stop the server.

### Logs

Each client logs to `<name>.log` in the project root, the terminal only shows the chat. The server logs to its terminal.

Logs from an example run with three clients are in the logs directory.
