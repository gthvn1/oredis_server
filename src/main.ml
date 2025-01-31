open Lwt.Infix (* Allow to use >>= *)

(* Server connection *)
let handle_connection socket =
  (* Read the request from client *)
  let buf_len = 1024 in
  let buf = Bytes.create buf_len in

  let rec read_loop () =
    Lwt_unix.read socket buf 0 buf_len >>= fun bytes_read ->
    if bytes_read = 0 then Lwt_io.eprintf "Client disconnected\n"
    else
      let req = Bytes.sub_string buf 0 bytes_read in
      Lwt_io.eprintf "Received %d bytes from client\n" bytes_read >>= fun () ->
      let answer = Redis.respond_to req in
      Lwt_unix.write socket (Bytes.of_string answer) 0 (String.length answer)
      >>= fun _ -> read_loop ()
  in
  read_loop () >>= fun () -> Lwt_unix.close socket

let server ~port =
  (* Create a TCP server socket: this is a synchrone function so no need to >>= *)
  let server_socket = Lwt_unix.(socket PF_INET SOCK_STREAM 0) in
  Lwt_unix.(setsockopt server_socket SO_REUSEADDR true);

  (* bind a socket to the localhost and port 6379: this one is asynchrone so we use >>= *)
  Lwt_unix.bind server_socket
    (Lwt_unix.ADDR_INET (Unix.inet_addr_of_string "127.0.0.1", port))
  >>= fun () ->
  (* Setup the socket to receive connection request.
     Currently we are accepting 5 pending request *)
  let max_pending_reqs = 5 in
  Lwt_unix.listen server_socket max_pending_reqs;

  Lwt_io.eprintf "Listenning on port %d!\n" port >>= fun () ->
  let rec accept_loop () =
    (* Accept connections on the given socket *)
    Lwt_unix.accept server_socket >>= fun (client_socket, _) ->
    Lwt.async (fun () -> handle_connection client_socket);
    accept_loop ()
  in
  accept_loop () >>= fun () -> Lwt_unix.close server_socket

(* Process that saves data on the disk *)
let rec save_db interval =
  Lwt_unix.sleep interval >>= fun () ->
  Redis.Rdb.save ();
  save_db interval

(* Main starts here with parameters *)
let usage_msg = "--dir /tmp/redis-files --dbfilename dump.rdb"
let dir_conf = ref "/tmp/redis-files"
let dbfilename_conf = ref "dump.rdb"
let port_conf = ref 6379

let conflist =
  [
    ( "--dir",
      Arg.Set_string dir_conf,
      "path to the directory where the RDB file is stored" );
    ("--dbfilename", Arg.Set_string dbfilename_conf, "the name of the RDB file");
    ("--port", Arg.Set_int port_conf, "Set port for redis server");
  ]

let () =
  Arg.parse conflist (fun _ -> ()) usage_msg;
  Redis.Conf.set_dir !dir_conf;
  Redis.Conf.set_dbfilename !dbfilename_conf;
  Redis.Rdb.load ();
  Lwt_main.run (Lwt.join [ server ~port:!port_conf; save_db 60.0 ])
