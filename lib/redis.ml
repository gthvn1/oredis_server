module Conf = Conf
open Resp

module Internal = struct
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

  let process (resp : t) (conf : Conf.t) : string =
    match resp with
    | Arrays elements ->
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
        Cmd.execute lst conf
    | _ -> raise_parse_error "Other request than arrays are not implemented"
end

let respond_to (req : string) (conf : Conf.t) : string =
  let open Printf in
  try
    printf "Parsing request ... \n";
    let resp = Internal.parse req in
    printf "  -> decoded (humain): %s\n" (to_human resp);
    printf "  -> decoded (string): %s\n" (to_string resp);
    flush_all ();
    Internal.process resp conf
  with Parse_error s -> s
