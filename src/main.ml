open Lwt.Infix (* Allow to use >>= *)

let handle_connection socket redis_conf =
  (* Read the request from client *)
  let buf_len = 1024 in
  let buf = Bytes.create buf_len in

  let rec read_loop () =
    Lwt_unix.read socket buf 0 buf_len >>= fun bytes_read ->
    if bytes_read = 0 then Lwt_io.eprintf "Client disconnected\n"
    else
      let req = Bytes.sub_string buf 0 bytes_read in
      Lwt_io.eprintf "Received %d bytes from client\n" bytes_read >>= fun () ->
      let answer = Redis.respond_to req redis_conf in
      Lwt_unix.write socket (Bytes.of_string answer) 0 (String.length answer)
      >>= fun _ -> read_loop ()
  in
  read_loop () >>= fun () -> Lwt_unix.close socket

let server redis_conf =
  (* Create a TCP server socket: this is a synchrone function so no need to >>= *)
  let server_socket = Lwt_unix.(socket PF_INET SOCK_STREAM 0) in
  Lwt_unix.(setsockopt server_socket SO_REUSEADDR true);

  (* bind a socket to the localhost and port 6379: this one is asynchrone so we use >>= *)
  Lwt_unix.bind server_socket
    (Lwt_unix.ADDR_INET (Unix.inet_addr_of_string "127.0.0.1", 6379))
  >>= fun () ->
  (* Setup the socket to receive connection request.
     Currently we are accepting 5 pending request *)
  let max_pending_reqs = 5 in
  Lwt_unix.listen server_socket max_pending_reqs;

  Lwt_io.eprintf "Listenning on port 6379!\n" >>= fun () ->
  let rec accept_loop () =
    (* Accept connections on the given socket *)
    Lwt_unix.accept server_socket >>= fun (client_socket, _) ->
    Lwt.async (fun () -> handle_connection client_socket redis_conf);
    accept_loop ()
  in
  accept_loop () >>= fun () -> Lwt_unix.close server_socket

(* Parameters *)
let usage_msg = "--dir /tmp/redis-files --dbfilename dump.rdb"
let dir_conf = ref "/tmp/redis-files"
let dbfilename_conf = ref "dump.rdb"

let conflist =
  [
    ( "--dir",
      Arg.Set_string dir_conf,
      "path to the directory where the RDB file is stored" );
    ("--dbfilename", Arg.Set_string dbfilename_conf, "the name of the RDB file");
  ]

let () =
  Arg.parse conflist (fun _ -> ()) usage_msg;
  let conf = Redis.Conf.create ~dir:!dir_conf ~dbfilename:!dbfilename_conf in
  Lwt_main.run @@ server conf
