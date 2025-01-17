type t = Ping | Echo | Set | Get

type value_options = {
  value : string;
  ex : int;
      (* Set the specified expire time, in seconds (a positive integer). *)
  px : int;
      (* Set the specified expire time, in milliseconds (a positive integer). *)
  exat : int;
      (* Set the specified Unix time at which the key will expire, in seconds (a positive integer). *)
  pxat : int;
      (* Set the specified Unix time at which the key will expire, in milliseconds (a positive integer). *)
  nx : bool; (* Only set the key if it does not already exist. *)
  xx : bool; (* Only set the key if it already exists. *)
  keepttl : bool; (* Retain the time to live associated with the key. *)
  get : bool;
      (* Return the old string stored at key, or nil if key did not exist. An error is returned and SET aborted if the value stored at key is not a string. *)
}

let db = Mem_storage.create ()

let create_value ?(ex = 0) ?(px = 0) ?(exat = 0) ?(pxat = 0) ?(nx = false)
    ?(xx = false) ?(keepttl = false) ?(get = false) (value : string) :
    value_options =
  { value; ex; px; exat; pxat; nx; xx; keepttl; get }

let of_string (str : string) : t option =
  let str = String.uppercase_ascii str in
  if str = "PING" then Some Ping
  else if str = "ECHO" then Some Echo
  else if str = "SET" then Some Set
  else if str = "GET" then Some Get
  else None

let execute (lst : string list) : string =
  let open Resp in
  match of_string (List.hd lst) with
  | None -> raise_parse_error "Unknown command"
  | Some Ping ->
      if List.length lst = 1 then Simple_strings "PONG" |> to_string
      else Bulk_strings (Some (List.nth lst 1)) |> to_string
  | Some Echo ->
      if List.length lst <> 2 then
        raise_parse_error "one argument is expected for echo";
      Bulk_strings (Some (List.nth lst 1)) |> to_string
  | Some Set ->
      if List.length lst <> 3 then
        raise_parse_error "key, value is expected for set";
      let value = create_value (List.nth lst 2) in
      Mem_storage.set db ~key:(List.nth lst 1) ~value;
      Simple_strings "OK" |> to_string
  | Some Get -> (
      if List.length lst <> 2 then raise_parse_error "key is expected for get";
      match Mem_storage.get db ~key:(List.nth lst 1) with
      | Some v -> Bulk_strings (Some v.value) |> to_string
      | None -> null_bulk_string)
