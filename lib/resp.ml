(* Link to REDIS Protocol: https://redis.io/docs/latest/develop/reference/protocol-spec/ *)

exception Parse_error of string

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
