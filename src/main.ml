open Unix

let () =
  (* Create a TCP server socket *)
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;

  (* bind a socket to the localhost and port 6379 *)
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", 6379));

  (* Setup the socket to receive connection request.
     Currently we are accepting 1 pending request *)
  listen server_socket 1;

  Printf.eprintf "Listenning on port 6379!\n";
  (* Ensure that the message is printed *)
  flush_all ();

  (* Accept connections on the given socket *)
  let client_socket, _client_addr = accept server_socket in

  (* Read the request from client *)
  let buf_len = 1024 in
  let buf = Bytes.create buf_len in

  let rec read_loop () =
    match read client_socket buf 0 buf_len with
    | 0 -> Printf.eprintf "Client disconnected"
    | bytes_read ->
        Printf.eprintf "Received %d bytes from client\n" bytes_read;
        let req = Bytes.sub_string buf 0 bytes_read in
        Printf.eprintf "Request is: %s\n" req;
        (* Currently we are only supporting PING *)
        let pong_str = "+PONG\r\n" in
        let _bytes_written =
          write client_socket (Bytes.of_string pong_str) 0
            (String.length pong_str)
        in
        read_loop ()
  in
  read_loop ();

  close client_socket;
  close server_socket
