type t = Ping | Echo | Set | Get | Config

module Set_options = struct
  type t = {
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
    creation_time : float; (* Seconds since epoch *)
  }

  let create_value ?(ex = 0) ?(px = 0) ?(exat = 0) ?(pxat = 0) ?(nx = false)
      ?(xx = false) ?(keepttl = false) ?(get = false) (value : string) : t =
    {
      value;
      ex;
      px;
      exat;
      pxat;
      nx;
      xx;
      keepttl;
      get;
      creation_time = Unix.gettimeofday ();
    }

  (** [of_string_list sl] builds a set options according to the string list
      [sl]. We are excpecting the value as the first element of the list and
      options follow. We are expecting something like:
      ["bar"; "nx"; "ex"; "1" ; "xx" ] *)
  let of_string_list (sl : string list) : t =
    if List.length sl < 1 then failwith "At least a value is expected";
    let v = List.hd sl in
    let get_opt = function
      | [] -> Resp.raise_parse_error "a value is expected "
      | x :: xs -> (
          match int_of_string_opt x with
          | None -> Resp.raise_parse_error "wrong ex value"
          | Some x -> (x, xs))
    in
    let rec loop (l : string list) (set_opt : t) : t =
      match l with
      | [] -> set_opt
      | o :: os -> (
          match String.uppercase_ascii o with
          | "EX" ->
              let v, rem = get_opt os in
              loop rem { set_opt with ex = v }
          | "PX" ->
              let v, rem = get_opt os in
              loop rem { set_opt with px = v }
          | "NX" -> loop os { set_opt with nx = true }
          | "XX" -> loop os { set_opt with xx = true }
          | "KEEPTTL" -> loop os { set_opt with keepttl = true }
          | "GET" -> loop os { set_opt with get = true }
          | _ ->
              Printf.eprintf "skip %s: unknown option\n" o;
              flush_all ();
              loop os set_opt)
    in
    loop (List.tl sl) (create_value v)
end

let db = Mem_storage.create ()

let of_string (str : string) : t option =
  let str = String.uppercase_ascii str in
  if str = "PING" then Some Ping
  else if str = "ECHO" then Some Echo
  else if str = "SET" then Some Set
  else if str = "GET" then Some Get
  else if str = "CONFIG" then Some Config
  else None

let ping_cmd lst =
  if List.is_empty lst then Resp.(Simple_strings "PONG" |> to_string)
  else Resp.(Bulk_strings (Some (List.hd lst)) |> to_string)

let echo_cmd lst =
  if List.length lst <> 1 then
    Resp.raise_parse_error "one argument is expected for echo";
  Resp.(Bulk_strings (Some (List.hd lst)) |> to_string)

let set_cmd lst =
  if List.length lst < 2 then
    Resp.raise_parse_error "At least key and value are expected for set";
  let key = List.hd lst in
  let value = Set_options.of_string_list (List.tl lst) in
  Mem_storage.set db ~key ~value;
  Resp.(Simple_strings "OK" |> to_string)

let get_cmd lst =
  let open Resp in
  if List.length lst <> 1 then raise_parse_error "key is expected for get";
  match Mem_storage.get db ~key:(List.hd lst) with
  | Some v ->
      (* Before returning the value check that it is still valid *)
      let now = Unix.gettimeofday () in
      if v.px <> 0 && v.creation_time +. (Float.of_int v.px /. 1000.0) < now
      then Resp.null_bulk_string
      else if v.ex <> 0 && v.creation_time +. Float.of_int v.ex < now then
        Resp.null_bulk_string
      else Bulk_strings (Some v.value) |> to_string
  | None -> null_bulk_string

let config_cmd lst conf =
  let open Resp in
  if List.is_empty lst then raise_parse_error "Expected a config subcommand";
  let subcmd = List.hd lst |> String.uppercase_ascii in
  let rest = List.tl lst in
  match subcmd with
  | "GET" ->
      if List.is_empty rest then
        raise_parse_error "CONFIG GET expects a parameter";
      let resp =
        match List.hd rest with
        | "dir" ->
            Arrays
              [ Bulk_strings (Some "dir"); Bulk_strings (Some (Conf.dir conf)) ]
        | "dbfilename" ->
            Arrays
              [
                Bulk_strings (Some "dbfilename");
                Bulk_strings (Some (Conf.dbfilename conf));
              ]
        | _ -> raise_parse_error "CONFIG GET unknown parameter"
      in
      resp |> to_string
  | _ -> raise_parse_error "Unknown config subcommand"

let execute (lst : string list) (conf : Conf.t) : string =
  if List.is_empty lst then
    Resp.raise_parse_error "Cannot execute an empty request";
  match of_string (List.hd lst) with
  | None -> Resp.raise_parse_error "Unknown command"
  | Some Ping -> ping_cmd (List.tl lst)
  | Some Echo -> echo_cmd (List.tl lst)
  | Some Set -> set_cmd (List.tl lst)
  | Some Get -> get_cmd (List.tl lst)
  | Some Config -> config_cmd (List.tl lst) conf
