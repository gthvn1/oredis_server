(* Link to REDIS Protocol: https://redis.io/docs/latest/develop/reference/protocol-spec/ *)

module Utils = struct
  (** [is_digit c] returns true if [c] is a digit. *)
  let is_digit c = Char.code '0' <= Char.code c && Char.code c <= Char.code '9'

  (** [to_digit_exn c] returns the integer value of the character [c] if it is a
      digit. It raises an exception otherwise. *)
  let to_digit_exn c =
    if is_digit c then Char.code c - Char.code '0'
    else failwith "Not a valid number"

  (** [split_on_crlf str] splits the string [str] on \r\n pattern. It returns
      the list of all strings found. *)
  let split_on_crlf (s : string) : string list =
    let l = String.to_seq s |> List.of_seq in
    let rec loop (cl : char list) (acc : char list list) (cur : char list) =
      match cl with
      | [] -> List.rev @@ (List.rev cur :: acc)
      | '\r' :: '\n' :: xs -> loop xs (List.rev cur :: acc) []
      | c :: xs -> loop xs acc (c :: cur)
    in
    loop l [] []
    |> List.map (fun l -> List.to_seq l |> String.of_seq)
    |> List.filter (fun s -> String.length s <> 0)

  (** [trim_crlf str] remove all crlf delimiters at the beginning of the string
      [str] if any .*)
  let rec trim_crlf (str : string) : string =
    if String.length str < 2 then str
    else if str.[0] = '\r' && str.[1] = '\n' then
      trim_crlf (String.sub str 2 (String.length str - 2))
    else str

  (** [print_as_char_lists str] print all characters code of a given string
      [str]. It can be use for debugging. *)
  let print_as_char_list (str : string) : unit =
    print_string ">>> ";
    str |> String.to_seq |> List.of_seq |> List.map Char.code
    |> List.iter (fun x -> Printf.printf "%d " x)
    |> print_newline

  (** [drop_from_start str ~drop] drops the first [drop] characters of the
      string [str]. *)
  let drop_from_start (str : string) ~(drop : int) : string =
    let sz = String.length str in
    if drop < sz then String.sub str drop (sz - drop) else ""

  (** [get_int_from_start str] returns the integer at the beginning of [str] if
      any and the rest of the string. *)
  let get_int_from_start (str : string) : int option * string =
    let cl = String.to_seq str |> List.of_seq in
    let rec loop (l : char list) (acc : int) =
      match l with
      | [] -> (Some acc, "")
      | x :: xs as s ->
          if is_digit x then loop xs ((acc * 10) + to_digit_exn x)
          else
            let rest = List.to_seq s |> String.of_seq in
            (Some acc, rest)
    in
    if is_digit (List.hd cl) then loop (List.tl cl) (to_digit_exn (List.hd cl))
    else (None, str)
end

exception Parse_error of string

module Resp = struct
  type t =
    | Simple_strings of string
    | Simple_errors of string
    | Integers
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
  let execute (resp : t) : string =
    match resp with
    | Arrays [ Bulk_strings (Some cmd) ] ->
        let cmd = String.uppercase_ascii cmd in
        if cmd = "PING" then Simple_strings "PONG" |> to_string
        else raise_parse_error "Command unknown"
    | Arrays [ Bulk_strings (Some cmd); Bulk_strings (Some param) ] ->
        let cmd = String.uppercase_ascii cmd in
        if cmd = "ECHO" || cmd = "PING" then
          Bulk_strings (Some param) |> to_string
        else raise_parse_error "Command unknown"
    | _ -> raise_parse_error "Not implemented"
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
