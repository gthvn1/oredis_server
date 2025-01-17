(* Link to REDIS Protocol: https://redis.io/docs/latest/develop/reference/protocol-spec/ *)

(* For the first implementation of SET/GET we are using in-memory storage. *)
let kv_store : (string, string) Hashtbl.t = Hashtbl.create 1024
let kv_mutex = Mutex.create ()

let set_kv (key : string) (value : string) : unit =
  Mutex.lock kv_mutex;
  Hashtbl.replace kv_store key value;
  Mutex.unlock kv_mutex

let get_kv (key : string) : string option =
  Mutex.lock kv_mutex;
  let result = Hashtbl.find_opt kv_store key in
  Mutex.unlock kv_mutex;
  result

exception Parse_error of string

module Cmd = struct
  type t = Ping | Echo | Set | Get

  let of_string (str : string) : t option =
    let str = String.uppercase_ascii str in
    if str = "PING" then Some Ping
    else if str = "ECHO" then Some Echo
    else if str = "SET" then Some Set
    else if str = "GET" then Some Get
    else None
end

module Resp = struct
  type t =
    | Simple_strings of string
    | Simple_errors of string
    | Integers of int
    | Bulk_strings of string option (* None is the empty string *)
    | Arrays of t list
    | Nulls
    | Booleans
    | Doubles
    | Big_numbers
    | Bulk_errors
    | Verbatim_strings
    | Maps
    | Attributes
    | Sets
    | Pushes

  let null_bulk_string = "$-1\r\n"

  let rec to_string = function
    | Simple_strings s -> "+" ^ s ^ "\r\n"
    | Simple_errors s -> "-ERR " ^ s ^ "\r\n"
    | Bulk_strings None -> "$0\r\n\r\n"
    | Bulk_strings (Some s) ->
        "$" ^ string_of_int (String.length s) ^ "\r\n" ^ s ^ "\r\n"
    | Arrays elmts ->
        "*"
        ^ string_of_int (List.length elmts)
        ^ "\r\n"
        ^ String.concat "" (List.map to_string elmts)
    | _ -> " <todo> "

  let rec to_human = function
    | Bulk_strings None -> "<bulk_strings: empty>"
    | Bulk_strings (Some s) -> "<bulk_strings:" ^ s ^ ">"
    | Arrays elmts ->
        let l = List.map to_human elmts in
        "<arrays: [" ^ String.concat " " l ^ "]>"
    | _ -> " <todo> "

  let raise_parse_error s =
    let err = Simple_errors s |> to_string in
    raise (Parse_error err)

  (* An array is encoding as follow: *<number-of-elements>\r\n<element-1>...<element-n>.
    Note: we already removed the '*' during the first step of the parsing. *)
  let rec parse_arrays (req : string) : t * string =
    (* get the number of elements *)
    match Utils.get_int_from_start req with
    | None, _ -> raise_parse_error "Size is missing for array"
    | Some sz, rest ->
        let rest = Utils.trim_crlf rest in
        if sz = 0 then (Arrays [], rest)
        else
          let rec loop (r : string) (n : int) (acc : t list) =
            if n = 0 then (Arrays (List.rev acc), r)
            else
              let value, remaining = parse_element r in
              loop (Utils.trim_crlf remaining) (n - 1) (value :: acc)
          in
          loop rest sz []

  (* A bulk string represents a single binary string: $<length>\r\n<data>\r\n.
  In our case the $ is not part of the request because we parse it to get here.*)
  and parse_bulk_strings (req : string) : t * string =
    match Utils.get_int_from_start req with
    | None, _ -> raise_parse_error "Failed to get lenght of the bulk string"
    | Some len, rest ->
        let rest = Utils.trim_crlf rest in
        if len > String.length rest then
          raise_parse_error "wrong len in bulk string"
        else if len = 0 then (Bulk_strings None, rest)
        else
          let str = String.sub rest 0 len in
          let rest = String.sub rest len (String.length rest - len) in
          (Bulk_strings (Some str), Utils.trim_crlf rest)

  and parse_element (req : string) : t * string =
    match req.[0] with
    | '*' -> parse_arrays (Utils.drop_from_start req ~drop:1)
    | '$' -> parse_bulk_strings (Utils.drop_from_start req ~drop:1)
    | _ ->
        Printf.printf "TODO: parse data type <%c>\n" req.[0];
        raise_parse_error "Unkown RESP data type"

  (** [parse req] is the entry point to parse a request [req] in Redis
      Serialization format. *)
  let parse (req : string) : t =
    if String.length req = 0 then raise_parse_error "empty request";
    let value, remaining = parse_element req in
    if String.length remaining = 0 then value
    else (
      Printf.eprintf "ERROR: Remaining data %s after parsing is not expected\n"
        remaining;
      raise_parse_error "remaining data")

  (* https://redis.io/docs/latest/commands/command-docs/ *)

  (* Currently we only have arrays of bulk strings. So check that the list is homogenous. *)
  let rec all_bulk_strings (lst : t list) : bool =
    match lst with
    | [] -> true
    | Bulk_strings _ :: xs -> all_bulk_strings xs
    | _ -> false

  let execute (resp : t) : string =
    match resp with
    | Arrays elements -> (
        if not (all_bulk_strings elements) then
          raise_parse_error "Only Arrays of bulk strings is implemented";

        let lst =
          List.map
            (fun b ->
              match b with
              | Bulk_strings None -> ""
              | Bulk_strings (Some s) -> s
              | _ ->
                  failwith
                    "unreachable because we checked that all elements are bulk \
                     strings")
            elements
        in
        match Cmd.of_string (List.hd lst) with
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
            set_kv (List.nth lst 1) (List.nth lst 2);
            Simple_strings "OK" |> to_string
        | Some Get -> (
            if List.length lst <> 2 then
              raise_parse_error "key is expected for get";
            match get_kv (List.nth lst 1) with
            | Some s -> Bulk_strings (Some s) |> to_string
            | None -> null_bulk_string))
    | _ -> raise_parse_error "Other request than arrays are not implemented"
end

let respond_to (req : string) : string =
  let open Printf in
  try
    printf "Parsing request ... \n";
    let resp = Resp.parse req in
    printf "  -> decoded (humain): %s\n" (Resp.to_human resp);
    printf "  -> decoded (string): %s\n" (Resp.to_string resp);
    flush_all ();
    Resp.execute resp
  with Parse_error s -> s
